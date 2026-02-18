import json
import os
import threading
import time
from datetime import datetime

import requests
from altfe.interface.root import classRoot, interRoot
from loguru import logger
from tqdm import tqdm


class CronMatcher:
    def __init__(self, expr: str):
        parts = str(expr or "").strip().split()
        if len(parts) != 5:
            raise ValueError("cron expression must contain 5 fields")
        self._fields = [
            self._parse_field(parts[0], 0, 59),   # minute
            self._parse_field(parts[1], 0, 23),   # hour
            self._parse_field(parts[2], 1, 31),   # day
            self._parse_field(parts[3], 1, 12),   # month
            self._parse_field(parts[4], 0, 7),    # weekday
        ]

    def matches(self, dt: datetime):
        cron_weekday = (dt.weekday() + 1) % 7
        vals = [dt.minute, dt.hour, dt.day, dt.month, cron_weekday]
        for idx in range(5):
            allowed = self._fields[idx]
            val = vals[idx]
            if idx == 4 and allowed is not None and 7 in allowed:
                allowed = set(allowed)
                allowed.add(0)
            if allowed is not None and val not in allowed:
                return False
        return True

    @staticmethod
    def _parse_field(token: str, min_v: int, max_v: int):
        token = token.strip()
        if token == "*":
            return None

        allowed = set()
        for seg in token.split(","):
            seg = seg.strip()
            if seg == "":
                raise ValueError(f"invalid cron field: '{token}'")

            step = 1
            base = seg
            if "/" in seg:
                base, step_s = seg.split("/", 1)
                step = int(step_s)
                if step <= 0:
                    raise ValueError(f"invalid cron step: '{seg}'")

            if base == "*":
                start, end = min_v, max_v
            elif "-" in base:
                start_s, end_s = base.split("-", 1)
                start, end = int(start_s), int(end_s)
            else:
                start = end = int(base)

            if start < min_v or end > max_v or start > end:
                raise ValueError(f"cron value out of range: '{seg}'")
            for v in range(start, end + 1, step):
                allowed.add(v)

        return allowed


@interRoot.bind("bookmarkSync", "LIB_COMMON")
class BookmarkSync(interRoot):
    def __init__(self):
        self.cron_expr = str(os.environ.get("BIU_SYNC_BOOKMARKS_CRON", "") or "").strip()
        self.webhook_url = str(os.environ.get("BIU_SYNC_WEBHOOK_URL", "") or "").strip()
        self.webhook_title = str(
            os.environ.get("BIU_SYNC_WEBHOOK_TITLE", "PixivBiu 收藏定时同步结果") or "PixivBiu 收藏定时同步结果"
        ).strip()
        self.webhook_content_url = str(os.environ.get("BIU_SYNC_WEBHOOK_CONTENT_URL", "") or "").strip()
        self.webhook_timeout_sec = max(3, int(os.environ.get("BIU_SYNC_WEBHOOK_TIMEOUT_SEC", "10")))

        self._matcher = None
        self._thread = None
        self._run_lock = threading.Lock()
        self._last_hit_minute = None

    def start(self):
        classRoot.setENV("bookmarkSyncInstance", self)
        if self.cron_expr == "":
            logger.info("Bookmark sync scheduler disabled (BIU_SYNC_BOOKMARKS_CRON not set).")
            return False
        try:
            self._matcher = CronMatcher(self.cron_expr)
        except Exception as e:
            logger.error("Bookmark sync scheduler disabled due to invalid cron '{}': {}", self.cron_expr, str(e))
            return False
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("Bookmark sync scheduler started. cron='{}'", self.cron_expr)
        return True

    def _loop(self):
        while True:
            now = datetime.now()
            key = now.strftime("%Y-%m-%d %H:%M")
            if key != self._last_hit_minute and self._matcher.matches(now):
                self._last_hit_minute = key
                self.run_now(async_mode=False, trigger="cron")
            time.sleep(5)

    def run_now(self, async_mode=True, trigger="manual"):
        if not self._run_lock.acquire(blocking=False):
            logger.warning("Bookmark sync job skipped because previous run is still in progress.")
            return False
        logger.info(
            "Bookmark sync job started. trigger={}, async={}, at={}",
            trigger,
            async_mode,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        if async_mode:
            threading.Thread(target=self._run_once_locked, daemon=True).start()
            return True
        self._run_once_locked()
        return True

    def _run_once_locked(self):
        try:
            self._run_once()
        except Exception as e:
            logger.exception("Bookmark sync run failed: {}", str(e))
        finally:
            self._run_lock.release()

    def _run_once_guarded(self):
        # Keep compatibility for older calls.
        if not self._run_lock.acquire(blocking=False):
            logger.warning("Bookmark sync job skipped because previous run is still in progress.")
            return False
        self._run_once_locked()
        return True

    def _run_once(self):
        all_marks = self._fetch_all_bookmarks()
        total_artworks = len(all_marks)
        new_synced_artworks = 0
        success_images = 0
        failed_images = 0
        scheduled = {}
        already_synced_artworks = 0

        dl_cls = classRoot.osGet("PLUGIN", "api/biu/do/dl/")
        if dl_cls is None:
            raise RuntimeError("download plugin not found: api/biu/do/dl/")
        downloader = dl_cls()

        logger.info("Bookmark sync total favorites fetched: {}", total_artworks)

        for artwork in all_marks:
            artwork_id = str(artwork.get("id", ""))
            image_total = self._image_count(artwork)
            if image_total <= 0:
                continue
            try:
                result = downloader.dl({"method": "cron_bookmark_sync"}, {
                    "kt": "cron_bookmark_sync",
                    "workID": "none",
                    "data": json.dumps(artwork),
                })
            except Exception as e:
                logger.warning("Schedule artwork failed: artwork={}, reason={}", artwork_id, str(e))
                failed_images += image_total
                continue

            if result != "running":
                if result is False or result == "error":
                    failed_images += image_total
                continue

            task_states = self.CORE.dl.status(artwork_id)
            if len(task_states) == 0:
                # IMMICH mode and all pages already synced.
                already_synced_artworks += 1
                continue

            scheduled[artwork_id] = len(task_states)
            new_synced_artworks += 1
            logger.info(
                "Incremental artwork scheduled for upload. artwork_id={}, image_tasks={}",
                artwork_id,
                len(task_states),
            )

        logger.info(
            "Bookmark sync need-to-sync artworks: {} (already_synced={})",
            new_synced_artworks,
            already_synced_artworks,
        )

        if len(scheduled) > 0:
            self._wait_all_finished_with_progress(list(scheduled.keys()))

        for artwork_id, task_count in scheduled.items():
            states = self.CORE.dl.status(artwork_id)
            if len(states) == 0:
                failed_images += task_count
                continue
            done = states.count("done")
            failed = states.count("failed")
            other = max(0, len(states) - done - failed)
            success_images += done
            failed_images += (failed + other)

        content = (
            f"当前总作品数: {total_artworks}\n"
            f"新增同步作品数: {new_synced_artworks}\n"
            f"同步失败数: {failed_images}\n"
            f"成功数: {success_images}"
        )
        description = (
            f"收藏定时同步完成（{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}），"
            f"共 {total_artworks} 个作品，新增同步 {new_synced_artworks} 个作品。"
        )
        self._send_webhook(self.webhook_title, description, content, self.webhook_content_url)
        logger.info(
            "Bookmark sync completed. total_artworks={}, new_artworks={}, success_images={}, failed_images={}",
            total_artworks,
            new_synced_artworks,
            success_images,
            failed_images,
        )

    def _wait_all_finished_with_progress(self, artwork_ids):
        progress = tqdm(total=len(artwork_ids), desc="[BookmarkSync] artworks", dynamic_ncols=True, leave=True)
        done = set()
        deadline = time.time() + 6 * 60 * 60
        while True:
            all_done = True
            for artwork_id in artwork_ids:
                if artwork_id in done:
                    continue
                states = self.CORE.dl.status(artwork_id)
                if any(x in ("running", "waiting", "unknown") for x in states):
                    all_done = False
                    continue
                done.add(artwork_id)
                progress.update(1)
                logger.info("Artwork sync finished. artwork_id={}, progress={}/{}", artwork_id, len(done), len(artwork_ids))
            if all_done or time.time() > deadline:
                if time.time() > deadline and len(done) < len(artwork_ids):
                    logger.warning(
                        "Bookmark sync wait timeout. finished={}/{}",
                        len(done),
                        len(artwork_ids),
                    )
                progress.close()
                return
            time.sleep(2)

    def _fetch_all_bookmarks(self):
        api = self.CORE.biu.api
        arg = {"user_id": api.user_id, "restrict": "public", "max_bookmark_id": None}
        all_marks = []
        while True:
            rep = api.user_bookmarks_illust(**arg)
            illusts = rep.get("illusts", [])
            if len(illusts) == 0:
                break
            all_marks.extend(illusts)
            next_url = rep.get("next_url")
            if not next_url:
                break
            arg = api.parse_qs(next_url)
        return all_marks

    @staticmethod
    def _image_count(artwork):
        t = str(artwork.get("type", ""))
        if t not in ("illust", "manga", "ugoira"):
            return 0
        if t == "ugoira":
            return 0
        pages = artwork.get("meta_pages", [])
        if isinstance(pages, list) and len(pages) > 0:
            return len(pages)
        return 1

    def _send_webhook(self, title: str, description: str, content: str, url: str):
        if self.webhook_url == "":
            logger.warning("Webhook skipped: BIU_SYNC_WEBHOOK_URL is not set.")
            return False
        payload = {
            "title": title,
            "description": description,
            "content": content,
            "url": url,
        }
        logger.info(
            "Webhook request sending. endpoint={}, timeout={}s, title={}, content_length={}",
            self.webhook_url,
            self.webhook_timeout_sec,
            title,
            len(content),
        )
        try:
            rep = requests.post(
                self.webhook_url,
                json=payload,
                timeout=self.webhook_timeout_sec,
            )
            if rep.status_code >= 400:
                logger.warning("Webhook failed: status={}, body={}", rep.status_code, rep.text[:200])
                return False
            logger.info("Webhook success: status={}, body={}", rep.status_code, rep.text[:200])
            return True
        except Exception as e:
            logger.warning("Webhook request error: {}", str(e))
            return False
