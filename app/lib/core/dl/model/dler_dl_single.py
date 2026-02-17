import os
import time

import requests

from altfe.interface.root import interRoot
from app.lib.core.dl.model.dler import Dler
from loguru import logger


class DlSingleDler(Dler):
    def __init__(self, url, folder="./downloads/", name=None, dlArgs=Dler.TEMP_dlArgs, dlRetryMax=2, callback=None):
        super(DlSingleDler, self).__init__(url, folder, name, dlArgs, dlRetryMax, callback)
        if self._dlSaveName is None:
            try:
                headers = {}
                with requests.head(self._dlUrl, headers=self._dlArgs["_headers"], **self._dlArgs["@requests"],
                                   allow_redirects=True) as rep:
                    headers = rep.headers
            finally:
                self._dlSaveName = Dler.get_dl_filename(self._dlUrl, headers)
        self._dlSaveUri = os.path.join(self._dlSaveDir, self._dlSaveName)
        self._dlFileSize = -1
        interRoot.STATIC.file.mkdir(self._dlSaveDir)

    def run(self):
        if self.status(Dler.CODE_WAIT):
            max_attempts = self._dlRetryMax + 1
            for attempt in range(1, max_attempts + 1):
                self.status(Dler.CODE_GOOD_RUNNING, True)
                if self.__download_single():
                    if self._dlFileSize != -1 and self._dlFileSize != os.path.getsize(self._dlSaveUri):
                        logger.error(
                            "Download size mismatch: url={}, save={}, expected={}, actual={}, attempt={}/{}",
                            self._dlUrl,
                            self._dlSaveUri,
                            self._dlFileSize,
                            os.path.getsize(self._dlSaveUri),
                            attempt,
                            max_attempts,
                        )
                    else:
                        self.status(Dler.CODE_GOOD_SUCCESS, True)
                        break
                if attempt >= max_attempts:
                    self.status(Dler.CODE_BAD_FAILED, True)
                    logger.error(
                        "Download failed after retries: url={}, save={}, retries={}",
                        self._dlUrl,
                        self._dlSaveUri,
                        self._dlRetryMax,
                    )
                    break
                wait_sec = min(2 * attempt, 10)
                logger.warning(
                    "Download failed, wait and retry: url={}, save={}, attempt={}/{}, wait={}s",
                    self._dlUrl,
                    self._dlSaveUri,
                    attempt,
                    max_attempts,
                    wait_sec,
                )
                time.sleep(wait_sec)
                self._dlRetryNum = attempt
        self.callback()

    def __download_single(self):
        """
        单线程下载。
        :return: bool
        """
        try:
            with requests.get(self._dlUrl, headers=self._dlArgs["_headers"], stream=True,
                              **self._dlArgs["@requests"]) as rep:
                if rep.status_code >= 400:
                    logger.error(
                        "Download HTTP error: url={}, status={}, save={}",
                        self._dlUrl,
                        rep.status_code,
                        self._dlSaveUri,
                    )
                    return False
                self._dlFileSize = int(rep.headers.get("Content-Length", -1))
                # Immich mode may clean temp files concurrently; ensure directory exists before writing.
                interRoot.STATIC.file.mkdir(self._dlSaveDir)
                with open(self._dlSaveUri, "wb", buffering=1024) as f:
                    for chunk in rep.iter_content(chunk_size=2048):
                        # 若 CODE_BAD，则退出
                        if self.status(Dler.CODE_BAD):
                            return False
                        # 流写入
                        if chunk:
                            f.write(chunk)
                        # 若 CODE_WAIT，则等待
                        while self.status(Dler.CODE_WAIT):
                            time.sleep(1)
            return True
        except Exception as e:
            logger.error(
                "Download request exception: url={}, save={}, reason={}",
                self._dlUrl,
                self._dlSaveUri,
                str(e),
            )
            return False
        finally:
            self._stuIngFileSize = -1
