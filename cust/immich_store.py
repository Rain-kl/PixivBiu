import hashlib
import os
import sqlite3
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
        return self.session.request(method, url, **kwargs)

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

    def create_stack(self, asset_ids: list[str]):
        if len(asset_ids) <= 1:
            return None
        payload = {"assetIds": asset_ids}
        rep = self._request("POST", "/stacks", json=payload)
        if rep.status_code >= 400:
            raise ImmichError(f"create stack failed ({rep.status_code}): {rep.text[:300]}")
        return rep.json()

    def get_stack(self, stack_id: str):
        rep = self._request("GET", f"/stacks/{stack_id}")
        if rep.status_code >= 400:
            raise ImmichError(f"get stack failed ({rep.status_code}): {rep.text[:300]}")
        return rep.json()

    def update_stack(self, id_: str, primary_asset_id: str):
        payload = {"primaryAssetId": primary_asset_id}
        rep = self._request("PUT", f"/stacks/{id_}", json=payload)
        if rep.status_code >= 400:
            raise ImmichError(f"update stack failed ({rep.status_code}): {rep.text[:300]}")
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
        self._album_cache = {}
        self._works = {}

        self.host_key = self._host_key()
        self._db_uri = os.path.join(self.root_path, "usr/cache/immich_record.db")
        self._tbl_art = f"artwork_upload_record_{self.host_key}"
        self._tbl_img = f"artwork_image_record_{self.host_key}"
        self._init_db()

        self._album_cache_uri = os.path.join(self.root_path, "usr/cache/immich_album_cache.json")
        self._load_album_cache()

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

    def prepare_work(self, ctx: dict):
        artwork_id = str(ctx["pixivArtworkId"])
        author_id = str(ctx.get("authorId", ""))
        total = int(ctx.get("totalPages", 0))
        now = int(time.time())
        with self._db_conn() as conn:
            conn.execute(
                f"""
                INSERT INTO {self._tbl_art}
                (pixivArtworkId, authorId, immichStackId, totalImageCount, successCount, createdAt, updatedAt, status)
                VALUES (?, ?, NULL, ?, 0, ?, ?, 'RUNNING')
                ON CONFLICT(pixivArtworkId) DO UPDATE SET
                  authorId=excluded.authorId,
                  totalImageCount=excluded.totalImageCount,
                  updatedAt=excluded.updatedAt,
                  status='RUNNING'
                """,
                (artwork_id, author_id, total, now, now),
            )

        with self._lock:
            work = self._works.setdefault(
                artwork_id,
                {
                    "expected": total,
                    "processed": set(),
                    "new_assets": set(),
                    "ctx": ctx,
                    "finalized": False,
                    "up_bar": None,
                },
            )
            work["expected"] = total
            work["ctx"] = ctx
            if work["up_bar"] is None:
                work["up_bar"] = tqdm(total=total, desc=f"[{artwork_id}] UP", dynamic_ncols=True, leave=True)

    def should_skip_page(self, ctx: dict):
        artwork_id = str(ctx["pixivArtworkId"])
        page_index = int(ctx["pageIndex"])
        row = self._fetchone(
            f"SELECT downloadSuccess, immichAssetId FROM {self._tbl_img} WHERE pixivArtworkId=? AND pageIndex=?",
            (artwork_id, page_index),
        )
        if row is None:
            return False, None
        if int(row[0]) == 1 and row[1]:
            return True, str(row[1])
        return False, None

    def upsert_page_pending(self, ctx: dict, image_name: str):
        artwork_id = str(ctx["pixivArtworkId"])
        page_index = int(ctx["pageIndex"])
        source_url = str(ctx.get("sourceUrl", ""))
        now = int(time.time())
        with self._db_conn() as conn:
            conn.execute(
                f"""
                INSERT INTO {self._tbl_img}
                (pixivArtworkId, pageIndex, imageName, downloadSuccess, immichAssetId, sourceUrl, createdAt, updatedAt)
                VALUES (?, ?, ?, 0, NULL, ?, ?, ?)
                ON CONFLICT(pixivArtworkId, pageIndex) DO UPDATE SET
                  imageName=excluded.imageName,
                  sourceUrl=excluded.sourceUrl,
                  updatedAt=excluded.updatedAt
                """,
                (artwork_id, page_index, image_name, source_url, now, now),
            )

    def mark_page_skipped(self, ctx: dict, asset_id: str):
        self._mark_processed(ctx, success=True, asset_id=asset_id, is_new_asset=False)

    def mark_page_failed(self, ctx: dict):
        artwork_id = str(ctx["pixivArtworkId"])
        page_index = int(ctx["pageIndex"])
        now = int(time.time())
        with self._db_conn() as conn:
            conn.execute(
                f"""
                INSERT INTO {self._tbl_img}
                (pixivArtworkId, pageIndex, imageName, downloadSuccess, immichAssetId, sourceUrl, createdAt, updatedAt)
                VALUES (?, ?, ?, 0, NULL, ?, ?, ?)
                ON CONFLICT(pixivArtworkId, pageIndex) DO UPDATE SET
                  downloadSuccess=0,
                  updatedAt=excluded.updatedAt
                """,
                (
                    artwork_id,
                    page_index,
                    str(ctx.get("imageName", "")),
                    str(ctx.get("sourceUrl", "")),
                    now,
                    now,
                ),
            )
        self._mark_processed(ctx, success=False)

    def register_asset(self, ctx: dict, asset_id: str):
        artwork_id = str(ctx["pixivArtworkId"])
        page_index = int(ctx["pageIndex"])
        image_name = str(ctx.get("imageName", ""))
        source_url = str(ctx.get("sourceUrl", ""))
        now = int(time.time())
        with self._db_conn() as conn:
            conn.execute(
                f"""
                INSERT INTO {self._tbl_img}
                (pixivArtworkId, pageIndex, imageName, downloadSuccess, immichAssetId, sourceUrl, createdAt, updatedAt)
                VALUES (?, ?, ?, 1, ?, ?, ?, ?)
                ON CONFLICT(pixivArtworkId, pageIndex) DO UPDATE SET
                  imageName=excluded.imageName,
                  downloadSuccess=1,
                  immichAssetId=excluded.immichAssetId,
                  sourceUrl=excluded.sourceUrl,
                  updatedAt=excluded.updatedAt
                """,
                (artwork_id, page_index, image_name, str(asset_id), source_url, now, now),
            )
        self._mark_processed(ctx, success=True, asset_id=str(asset_id), is_new_asset=True)

    def upload_with_retry(self, file_path: str, file_name: str, ctx: dict):
        device_asset_id = f"pixiv-{ctx['pixivArtworkId']}-{ctx['pageIndex']}"
        err = None
        for idx in range(self.config.retry_max):
            try:
                return self.client.upload_asset(file_path=file_path, filename=file_name, device_asset_id=device_asset_id)
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

    def finalize_if_no_pending(self, artwork_id: str):
        with self._lock:
            work = self._works.get(str(artwork_id))
            if not work:
                return
            if work["finalized"]:
                return
            if len(work["processed"]) >= int(work["expected"]):
                work["finalized"] = True
            else:
                return
        self._finalize_work(str(artwork_id))

    def _mark_processed(self, ctx: dict, success: bool, asset_id: str | None = None, is_new_asset: bool = False):
        artwork_id = str(ctx["pixivArtworkId"])
        page_index = int(ctx["pageIndex"])
        should_finalize = False
        with self._lock:
            work = self._works.get(artwork_id)
            if not work:
                # tolerate callbacks after process restart
                self.prepare_work(ctx)
                work = self._works.get(artwork_id)
            if page_index not in work["processed"]:
                work["processed"].add(page_index)
            if success and work["up_bar"] is not None:
                # Progress means page is already available in Immich (new upload or skipped).
                work["up_bar"].update(1)
            if is_new_asset and asset_id:
                work["new_assets"].add(str(asset_id))
            if not work["finalized"] and len(work["processed"]) >= int(work["expected"]):
                work["finalized"] = True
                should_finalize = True
        if should_finalize:
            self._finalize_work(artwork_id)

    def _finalize_work(self, artwork_id: str):
        ctx = None
        new_assets = []
        with self._lock:
            work = self._works.get(str(artwork_id))
            if work:
                ctx = work.get("ctx")
                new_assets = list(work.get("new_assets", set()))
                self._close_progress_locked(work)

        all_assets = self._get_all_asset_ids(artwork_id)
        self._refresh_artwork_success(artwork_id)
        self._set_artwork_status(artwork_id, "FINISHED")

        if not all_assets:
            logger.info("Finalize skip stack: artwork={}, reason=no assets", artwork_id)
            return

        stack_id = self._get_stack_id(artwork_id)
        if stack_id:
            if new_assets:
                try:
                    new_stack_id = self._merge_into_existing_stack(stack_id, sorted(new_assets))
                    if new_stack_id and new_stack_id != stack_id:
                        self._set_stack_id(artwork_id, new_stack_id)
                    logger.info(
                        "Stack append success: artwork={}, stack_id={}, add_assets={}",
                        artwork_id,
                        new_stack_id if new_stack_id else stack_id,
                        len(new_assets),
                    )
                except Exception as e:
                    logger.error(
                        "Stack append failed: artwork={}, stack_id={}, reason={}", artwork_id, stack_id, str(e)
                    )
            else:
                logger.info("Stack exists and no new assets: artwork={}, stack_id={}", artwork_id, stack_id)
        else:
            if len(all_assets) > 1:
                rep = self.client.create_stack(asset_ids=all_assets)
                new_stack_id = None
                if isinstance(rep, dict):
                    new_stack_id = rep.get("id") or rep.get("stackId")
                if new_stack_id:
                    self._set_stack_id(artwork_id, str(new_stack_id))
                logger.info(
                    "Stack created: artwork={}, stack_id={}, assets={}",
                    artwork_id,
                    new_stack_id if new_stack_id else "unknown",
                    len(all_assets),
                )

        if ctx is not None:
            album_id, is_new = self._get_or_create_album(ctx.get("authorName", ""), ctx.get("authorId", ""), all_assets)
            if album_id and all_assets and not is_new:
                self.client.add_assets_to_album(album_id, all_assets)
                logger.info(
                    "Added assets to album: artwork={}, album_id={}, asset_count={}",
                    artwork_id,
                    album_id,
                    len(all_assets),
                )

    def _merge_into_existing_stack(self, stack_id: str, new_assets: list[str]):
        stack = self.client.get_stack(stack_id)
        existing_assets = [str(x.get("id")) for x in stack.get("assets", []) if x.get("id")]
        primary_asset_id = stack.get("primaryAssetId")
        if not primary_asset_id and existing_assets:
            primary_asset_id = existing_assets[0]

        if not primary_asset_id:
            merged = []
            seen = set()
            for x in existing_assets + new_assets:
                if x and x not in seen:
                    seen.add(x)
                    merged.append(x)
            if len(merged) <= 1:
                return stack_id
            rep = self.client.create_stack(asset_ids=merged)
            if isinstance(rep, dict):
                return str(rep.get("id") or rep.get("stackId") or stack_id)
            return stack_id

        unresolved = []
        existing_set = set(existing_assets)
        for asset_id in new_assets:
            if asset_id in existing_set:
                continue
            try:
                self.client.update_stack(id_=asset_id, primary_asset_id=str(primary_asset_id))
            except Exception:
                unresolved.append(asset_id)

        if len(unresolved) == 0:
            return stack_id

        merged = []
        seen = set()
        for x in existing_assets + new_assets:
            if x and x not in seen:
                seen.add(x)
                merged.append(x)
        if len(merged) <= 1:
            return stack_id

        rep = self.client.create_stack(asset_ids=merged)
        if isinstance(rep, dict):
            return str(rep.get("id") or rep.get("stackId") or stack_id)
        return stack_id

    def _host_key(self):
        host = (self.config.host or "").strip().rstrip("/")
        token = self.config.api_key or ""
        raw = f"{host}|{token}".encode("utf-8")
        return hashlib.sha1(raw).hexdigest()[:12]

    def _init_db(self):
        os.makedirs(os.path.dirname(self._db_uri), exist_ok=True)
        with self._db_conn() as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._tbl_art} (
                    pixivArtworkId TEXT PRIMARY KEY,
                    authorId TEXT,
                    immichStackId TEXT,
                    totalImageCount INTEGER NOT NULL DEFAULT 0,
                    successCount INTEGER NOT NULL DEFAULT 0,
                    createdAt INTEGER,
                    updatedAt INTEGER,
                    status TEXT
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._tbl_img} (
                    pixivArtworkId TEXT NOT NULL,
                    pageIndex INTEGER NOT NULL,
                    imageName TEXT,
                    downloadSuccess INTEGER NOT NULL DEFAULT 0,
                    immichAssetId TEXT,
                    sourceUrl TEXT,
                    createdAt INTEGER,
                    updatedAt INTEGER,
                    PRIMARY KEY (pixivArtworkId, pageIndex)
                )
                """
            )
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.host_key}_art_author ON {self._tbl_art}(authorId)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.host_key}_art_stack ON {self._tbl_art}(immichStackId)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.host_key}_img_art ON {self._tbl_img}(pixivArtworkId)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.host_key}_img_asset ON {self._tbl_img}(immichAssetId)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.host_key}_img_success ON {self._tbl_img}(downloadSuccess)")

    def _db_conn(self):
        conn = sqlite3.connect(self._db_uri, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _fetchone(self, sql: str, params: tuple):
        with self._db_conn() as conn:
            cur = conn.execute(sql, params)
            return cur.fetchone()

    def _get_all_asset_ids(self, artwork_id: str):
        with self._db_conn() as conn:
            cur = conn.execute(
                f"SELECT immichAssetId FROM {self._tbl_img} WHERE pixivArtworkId=? AND immichAssetId IS NOT NULL AND immichAssetId != '' ORDER BY pageIndex ASC",
                (str(artwork_id),),
            )
            return [str(x[0]) for x in cur.fetchall()]

    def _refresh_artwork_success(self, artwork_id: str):
        with self._db_conn() as conn:
            cur = conn.execute(
                f"SELECT COUNT(1) FROM {self._tbl_img} WHERE pixivArtworkId=? AND downloadSuccess=1 AND immichAssetId IS NOT NULL",
                (str(artwork_id),),
            )
            success = int(cur.fetchone()[0])
            conn.execute(
                f"UPDATE {self._tbl_art} SET successCount=?, updatedAt=? WHERE pixivArtworkId=?",
                (success, int(time.time()), str(artwork_id)),
            )

    def _set_artwork_status(self, artwork_id: str, status: str):
        with self._db_conn() as conn:
            conn.execute(
                f"UPDATE {self._tbl_art} SET status=?, updatedAt=? WHERE pixivArtworkId=?",
                (status, int(time.time()), str(artwork_id)),
            )

    def _get_stack_id(self, artwork_id: str):
        row = self._fetchone(
            f"SELECT immichStackId FROM {self._tbl_art} WHERE pixivArtworkId=?",
            (str(artwork_id),),
        )
        if not row or not row[0]:
            return None
        return str(row[0])

    def _set_stack_id(self, artwork_id: str, stack_id: str):
        with self._db_conn() as conn:
            conn.execute(
                f"UPDATE {self._tbl_art} SET immichStackId=?, updatedAt=? WHERE pixivArtworkId=?",
                (str(stack_id), int(time.time()), str(artwork_id)),
            )

    def _cache_key(self):
        token_hash = hashlib.sha1(self.config.api_key.encode("utf-8")).hexdigest()[:12]
        return f"{self.config.host}|{self.config.base_path}|{token_hash}"

    def _load_album_cache(self):
        if not os.path.exists(self._album_cache_uri):
            return
        try:
            import json

            with open(self._album_cache_uri, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except Exception:
            return
        if raw.get("config") != self._cache_key():
            return
        self._album_cache = raw.get("author_to_album", {}) or {}

    def _save_album_cache(self):
        import json

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

    def _close_progress_locked(self, work: dict):
        bar = work.get("up_bar")
        if bar is not None:
            try:
                bar.close()
            except Exception:
                pass
            work["up_bar"] = None
