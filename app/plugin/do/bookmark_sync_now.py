from altfe.interface.root import interRoot


@interRoot.bind("api/biu/do/bookmark_sync_now/", "PLUGIN")
class doBookmarkSyncNow(interRoot):
    def run(self, cmd):
        try:
            self.STATIC.arg.getArgs("bookmark_sync_now", li=["pass"], way="POST")
        except:
            return {"code": 0, "msg": "missing parameters"}

        syncer = self.getENV("bookmarkSyncInstance")
        if syncer is None:
            syncer = self.COMMON.bookmarkSync()
            self.setENV("bookmarkSyncInstance", syncer)

        if syncer.run_now(async_mode=True):
            return {"code": 1, "msg": "started"}
        return {"code": 0, "msg": "already running"}
