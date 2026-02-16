import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any

import requests
from tqdm import tqdm

try:
    from loguru import logger
except Exception:
    import logging

    logger = logging.getLogger("pixivbiu.immich")


class ImmichError(Exception):
    pass


@dataclass
class ImmichConfig:
    host: str
    api_key: str
    verify_tls: bool = True
    base_path: str = "/api"
    timeout_sec: int = 20
    retry_max: int = 3


class ImmichClient:
    def __init__(self, config: ImmichConfig):
        self.config = config
        host = (config.host or "").strip().rstrip("/")
        base = (config.base_path or "/api").strip()
        if not base.startswith("/"):
            base = "/" + base
        self.base_url = f"{host}{base}"
        self.session = requests.Session()
        self.session.headers.update({"x-api-key": config.api_key, "Accept": "application/json"})

    def _request(self, method: str, path: str, **kwargs):
        url = self.base_url + path
        kwargs.setdefault("timeout", self.config.timeout_sec)
        kwargs.setdefault("verify", self.config.verify_tls)
        rep = self.session.request(method, url, **kwargs)
        return rep

    def check_auth(self):
        rep = self._request("GET", "/users/me")
        if rep.status_code >= 400:
            raise ImmichError(f"auth failed ({rep.status_code})")
        return rep.json()

    def upload_asset(self, file_path: str, filename: str, device_asset_id: str):
        with open(file_path, "rb") as f:
            files = {"assetData": (filename, f)}
            data = {
                "deviceAssetId": device_asset_id,
                "deviceId": "PixivBiu",
                "fileCreatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "fileModifiedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            rep = self._request("POST", "/assets", files=files, data=data)
        if rep.status_code >= 400:
            raise ImmichError(f"upload failed ({rep.status_code}): {rep.text[:300]}")
        body = rep.json()
        asset_id = body.get("id") or body.get("assetId")
        if not asset_id:
            raise ImmichError(f"upload response missing asset id: {body}")
        return body

    def list_albums(self):
        rep = self._request("GET", "/albums")
        if rep.status_code >= 400:
            raise ImmichError(f"list albums failed ({rep.status_code})")
        return rep.json()

    def create_album(self, album_name: str, asset_ids: list[str] | None = None):
        payload = {"albumName": album_name}
        if asset_ids:
            payload["assetIds"] = asset_ids
        rep = self._request("POST", "/albums", json=payload)
        if rep.status_code >= 400:
            raise ImmichError(f"create album failed ({rep.status_code}): {rep.text[:300]}")
        return rep.json()

    def add_assets_to_album(self, album_id: str, asset_ids: list[str]):
        rep = self._request("PUT", f"/albums/{album_id}/assets", json={"ids": asset_ids})
        if rep.status_code >= 400:
            raise ImmichError(f"add assets to album failed ({rep.status_code}): {rep.text[:300]}")
        return rep.json()

    def create_stack(self, asset_ids: list[str], primary_asset_id: str):
        if len(asset_ids) <= 1:
            return None
        payload = {"assetIds": asset_ids, "primaryAssetId": primary_asset_id}
        rep = self._request("POST", "/stacks", json=payload)
        if rep.status_code >= 400:
            raise ImmichError(f"create stack failed ({rep.status_code}): {rep.text[:300]}")
        return rep.json()


class ImmichStore:
    def __init__(self, root_path: str, config_dict: dict[str, Any]):
        immich_set = config_dict.get("biu", {}).get("download", {}).get("immich", {})
        self.storage_mode = str(config_dict.get("biu", {}).get("download", {}).get("storageMode", "LOCAL")).upper()
        self.config = ImmichConfig(
            host=str(immich_set.get("host", "") or "").strip(),
            api_key=str(immich_set.get("apiKey", "") or "").strip(),
            verify_tls=bool(immich_set.get("verifyTLS", True)),
            base_path=str(immich_set.get("basePath", "/api") or "/api").strip(),
            timeout_sec=int(immich_set.get("timeoutSec", 20)),
            retry_max=max(1, int(immich_set.get("retryMax", 3))),
        )
        self.root_path = root_path
        self.client = ImmichClient(self.config)
        self._lock = threading.Lock()
        self._works = {}
        self._album_cache = {}
        self._album_cache_uri = os.path.join(self.root_path, "usr/cache/immich_album_cache.json")
        self._load_album_cache()
        logger.debug("ImmichStore initialized: host={}, base_path={}", self.config.host, self.config.base_path)

    def enabled(self):
        return self.storage_mode == "IMMICH"

    def configured(self):
        return bool(self.config.host and self.config.api_key)

    def check_connection(self):
        if not self.configured():
            return False, "missing immich.host or immich.apiKey"
        try:
            self.client.check_auth()
            logger.info("Immich auth check passed.")
            return True, "ok"
        except Exception as e:
            logger.error("Immich auth check failed: {}", str(e))
            return False, str(e)

    @staticmethod
    def sanitize_name(name: str, default: str = "unknown", limit: int = 120):
        cleaned = "".join(ch for ch in str(name) if ch not in "\\/:*?\"<>|").strip()
        if not cleaned:
            cleaned = default
        return cleaned[:limit]

    def make_album_name(self, author_name: str, author_id: str):
        return self.sanitize_name(f"{author_name} ({author_id})", default=f"author-{author_id}")

    def build_temp_folder(self, artwork_id: str):
        folder = os.path.join(self.root_path, f"usr/cache/immich/{artwork_id}/")
        os.makedirs(folder, exist_ok=True)
        return folder if folder.endswith("/") else folder + "/"

    def register_asset(self, ctx: dict, asset_id: str):
        if "pixivArtworkId" not in ctx or "totalPages" not in ctx or "pageIndex" not in ctx:
            logger.error("register_asset skipped due to invalid ctx: {}", ctx)
            return
        artwork_id = str(ctx["pixivArtworkId"])
        with self._lock:
            work = self._ensure_work(ctx)
            work["assets"][int(ctx["pageIndex"])] = str(asset_id)
            if work["up_bar"] is not None:
                work["up_bar"].update(1)
            can_finalize = (
                not work["finalized"]
                and not work["failed"]
                and len(work["assets"]) == work["expected"]
            )
            if can_finalize:
                work["finalized"] = True
        if can_finalize:
            self._finalize_work(artwork_id)

    def mark_failed(self, artwork_id: str):
        with self._lock:
            work = self._works.setdefault(
                str(artwork_id),
                {
                    "expected": 0,
                    "assets": {},
                    "finalized": True,
                    "failed": True,
                    "ctx": {},
                    "up_bar": None,
                },
            )
            work["failed"] = True
            self._close_progress_locked(work)

    def upload_with_retry(self, file_path: str, file_name: str, ctx: dict):
        device_asset_id = f"pixiv-{ctx['pixivArtworkId']}-{ctx['pageIndex']}"
        err = None
        for idx in range(self.config.retry_max):
            try:
                return self.client.upload_asset(
                    file_path=file_path,
                    filename=file_name,
                    device_asset_id=device_asset_id,
                )
            except Exception as e:
                err = e
                logger.warning(
                    "Immich upload retry {}/{} failed: artwork={}, page={}, reason={}",
                    idx + 1,
                    self.config.retry_max,
                    ctx.get("pixivArtworkId", "unknown"),
                    ctx.get("pageIndex", "unknown"),
                    str(e),
                )
                if idx + 1 >= self.config.retry_max:
                    break
                time.sleep(min(2 * (idx + 1), 5))
        raise ImmichError(str(err))

    def _finalize_work(self, artwork_id: str):
        with self._lock:
            work = self._works.get(str(artwork_id))
            if not work or work.get("failed"):
                return
            ctx = work["ctx"]
            assets = [work["assets"][x] for x in sorted(work["assets"])]
            self._close_progress_locked(work)

        if len(assets) > 1:
            rep = self.client.create_stack(asset_ids=assets, primary_asset_id=assets[0])
            logger.info(
                "Stack created successfully: artwork={}, primary={}, assets={}, stack={}",
                artwork_id,
                assets[0],
                len(assets),
                rep.get("id") if type(rep) is dict else "unknown",
            )

        album_id, is_new = self._get_or_create_album(ctx["authorName"], ctx["authorId"], assets)
        if album_id and assets and not is_new:
            self.client.add_assets_to_album(album_id, assets)
            logger.info(
                "Added assets to album: artwork={}, album_id={}, asset_count={}",
                artwork_id,
                album_id,
                len(assets),
            )

    def _cache_key(self):
        token_hash = hashlib.sha1(self.config.api_key.encode("utf-8")).hexdigest()[:12]
        return f"{self.config.host}|{self.config.base_path}|{token_hash}"

    def _load_album_cache(self):
        if not os.path.exists(self._album_cache_uri):
            return
        try:
            with open(self._album_cache_uri, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except Exception:
            return
        if raw.get("config") != self._cache_key():
            return
        self._album_cache = raw.get("author_to_album", {}) or {}

    def _save_album_cache(self):
        os.makedirs(os.path.dirname(self._album_cache_uri), exist_ok=True)
        data = {"config": self._cache_key(), "author_to_album": self._album_cache}
        with open(self._album_cache_uri, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    def _get_or_create_album(self, author_name: str, author_id: str, init_assets: list[str]):
        author_key = f"{author_id}"
        album_name = self.make_album_name(author_name, author_id)

        with self._lock:
            if author_key in self._album_cache:
                logger.info("Album already exists in cache: author_id={}, album_id={}", author_id, self._album_cache[author_key])
                return self._album_cache[author_key], False

        albums = self.client.list_albums()
        for x in albums:
            if x.get("albumName") == album_name and x.get("id"):
                with self._lock:
                    self._album_cache[author_key] = x["id"]
                    self._save_album_cache()
                logger.info(
                    "Album already exists: author_id={}, album_name={}, album_id={}",
                    author_id,
                    album_name,
                    x["id"],
                )
                return x["id"], False

        album = self.client.create_album(album_name=album_name, asset_ids=init_assets)
        album_id = album.get("id")
        if not album_id:
            raise ImmichError(f"create album missing id: {album}")
        with self._lock:
            self._album_cache[author_key] = album_id
            self._save_album_cache()
        logger.info(
            "Album created successfully: author_id={}, album_name={}, album_id={}, initial_assets={}",
            author_id,
            album_name,
            album_id,
            len(init_assets),
        )
        return album_id, True

    def _ensure_work(self, ctx: dict):
        artwork_id = str(ctx["pixivArtworkId"])
        work = self._works.setdefault(
            artwork_id,
            {
                "expected": int(ctx["totalPages"]),
                "assets": {},
                "finalized": False,
                "failed": False,
                "ctx": ctx,
                "up_bar": None,
            },
        )
        if work["up_bar"] is None:
            expected = int(work["expected"])
            prefix = f"[{artwork_id}]"
            work["up_bar"] = tqdm(
                total=expected,
                desc=f"{prefix} UP",
                dynamic_ncols=True,
                leave=True,
            )
        return work

    def _close_progress_locked(self, work: dict):
        bar = work.get("up_bar")
        if bar is not None:
            try:
                bar.close()
            except Exception:
                pass
            work["up_bar"] = None
