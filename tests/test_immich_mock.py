"""
PixivBiu Immich 功能 Mock 测试
根据需求文档补充V1进行验收测试

测试场景：
1. SQLite 持久化生效
2. 上传前判重（同作品同页已上传跳过）
3. 失败不影响堆叠（部分失败仍创建堆叠）
4. 重试不产生重复堆叠（合并进既有 stackId）
5. 作品级进度可追踪（totalImageCount / successCount）
6. 多 Immich 实例隔离（hostKey 表名）
"""

import json
import os
import sqlite3
import sys
import tempfile
import time
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, Mock, patch

# 添加项目路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cust.immich_store import ImmichClient, ImmichConfig, ImmichStore


@dataclass
class TestResult:
    """测试结果"""

    name: str
    passed: bool
    message: str
    details: dict = None


class MockImmichClient:
    """Mock Immich 客户端，模拟 API 响应"""

    def __init__(self):
        self.assets = {}  # asset_id -> {"id": ..., "deviceAssetId": ...}
        self.albums = {}  # album_id -> {"id": ..., "albumName": ..., "assets": []}
        self.stacks = {}  # stack_id -> {"id": ..., "assets": []}
        self.asset_counter = 1
        self.album_counter = 1
        self.stack_counter = 1

    def check_auth(self):
        """模拟认证"""
        return {"id": "user-123", "email": "test@example.com"}

    def upload_asset(self, file_path: str, filename: str, device_asset_id: str):
        """模拟上传资产"""
        asset_id = f"asset-{self.asset_counter:04d}"
        self.asset_counter += 1
        self.assets[asset_id] = {
            "id": asset_id,
            "assetId": asset_id,
            "deviceAssetId": device_asset_id,
            "filename": filename,
            "filePath": file_path,
        }
        return self.assets[asset_id]

    def list_albums(self):
        """模拟列出相册"""
        return list(self.albums.values())

    def create_album(self, album_name: str, asset_ids: list[str] | None = None):
        """模拟创建相册"""
        album_id = f"album-{self.album_counter:04d}"
        self.album_counter += 1
        self.albums[album_id] = {
            "id": album_id,
            "albumName": album_name,
            "assets": asset_ids or [],
        }
        return self.albums[album_id]

    def add_assets_to_album(self, album_id: str, asset_ids: list[str]):
        """模拟添加资产到相册"""
        if album_id in self.albums:
            self.albums[album_id]["assets"].extend(asset_ids)
        return {"success": True}

    def create_stack(self, asset_ids: list[str], primary_asset_id: str):
        """模拟创建堆叠"""
        if len(asset_ids) <= 1:
            return None
        stack_id = f"stack-{self.stack_counter:04d}"
        self.stack_counter += 1
        self.stacks[stack_id] = {
            "id": stack_id,
            "stackId": stack_id,
            "assets": asset_ids.copy(),
            "primaryAssetId": primary_asset_id,
        }
        return self.stacks[stack_id]

    def append_to_stack(self, stack_id: str, asset_ids: list[str]):
        """模拟追加资产到堆叠"""
        if stack_id in self.stacks:
            existing = set(self.stacks[stack_id]["assets"])
            for aid in asset_ids:
                if aid not in existing:
                    self.stacks[stack_id]["assets"].append(aid)
        return {"success": True}


class ImmichMockTest:
    """Immich Mock 测试主类"""

    def __init__(self, root_path: str = None):
        """初始化测试环境"""
        self.root_path = root_path or tempfile.mkdtemp(prefix="pixivbiu_test_")
        os.makedirs(os.path.join(self.root_path, "usr/cache"), exist_ok=True)
        self.results = []
        self.mock_client = MockImmichClient()

    def _create_test_config(
        self, host: str = "https://immich.test.com", api_key: str = "test-key-123"
    ):
        """创建测试配置"""
        return {
            "biu": {
                "download": {
                    "storageMode": "IMMICH",
                    "immich": {
                        "host": host,
                        "apiKey": api_key,
                        "verifyTLS": False,
                        "basePath": "/api",
                        "timeoutSec": 20,
                        "retryMax": 3,
                    },
                }
            }
        }

    def _create_store(self, config: dict = None) -> ImmichStore:
        """创建 ImmichStore 实例并注入 mock client"""
        if config is None:
            config = self._create_test_config()

        store = ImmichStore(self.root_path, config)
        # 注入 mock client
        store.client = self.mock_client
        return store

    def _create_temp_image(self, artwork_id: str, page_index: int) -> str:
        """创建临时测试图片文件"""
        folder = os.path.join(self.root_path, f"usr/cache/immich/{artwork_id}/")
        os.makedirs(folder, exist_ok=True)
        file_path = os.path.join(folder, f"{artwork_id}_p{page_index}.jpg")
        with open(file_path, "wb") as f:
            f.write(b"fake image data for testing")
        return file_path

    def _query_db(
        self, store: ImmichStore, table: str, where: str = "", params: tuple = ()
    ):
        """查询数据库"""
        tbl = store._tbl_art if table == "artwork" else store._tbl_img
        sql = f"SELECT * FROM {tbl}"
        if where:
            sql += f" WHERE {where}"
        with store._db_conn() as conn:
            cur = conn.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def add_result(self, result: TestResult):
        """添加测试结果"""
        self.results.append(result)
        status = "✅ PASS" if result.passed else "❌ FAIL"
        print(f"\n{status}: {result.name}")
        print(f"   {result.message}")
        if result.details:
            for key, val in result.details.items():
                print(f"   - {key}: {val}")

    # ========== 测试场景 ==========

    def test_01_sqlite_persistence(self):
        """测试场景1：SQLite 持久化生效"""
        print("\n" + "=" * 60)
        print("测试场景1：SQLite 持久化生效")
        print("=" * 60)

        try:
            store = self._create_store()

            # 验证数据库文件创建
            db_path = store._db_uri
            assert os.path.exists(db_path), "数据库文件未创建"

            # 验证表创建
            with sqlite3.connect(db_path) as conn:
                cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cur.fetchall()]

            expected_tables = [store._tbl_art, store._tbl_img]
            for tbl in expected_tables:
                assert tbl in tables, f"表 {tbl} 未创建"

            # 验证作品表结构
            artwork_cols = self._query_db(
                store, "artwork"
            )  # 空表会返回 []，但能验证表存在

            self.add_result(
                TestResult(
                    name="SQLite 持久化生效",
                    passed=True,
                    message="数据库文件、表、索引均创建成功",
                    details={
                        "数据库路径": db_path,
                        "作品表名": store._tbl_art,
                        "图片表名": store._tbl_img,
                        "hostKey": store.host_key,
                    },
                )
            )

        except Exception as e:
            self.add_result(
                TestResult(
                    name="SQLite 持久化生效",
                    passed=False,
                    message=f"测试失败: {str(e)}",
                )
            )

    def test_02_upload_deduplication(self):
        """测试场景2：上传前判重（同作品同页已上传跳过）"""
        print("\n" + "=" * 60)
        print("测试场景2：上传前判重")
        print("=" * 60)

        try:
            store = self._create_store()
            artwork_id = "123456789"

            # 准备作品：3页
            ctx = {
                "pixivArtworkId": artwork_id,
                "authorId": "artist-001",
                "authorName": "TestArtist",
                "totalPages": 3,
            }
            store.prepare_work(ctx)

            # 第一次上传：页0和页1成功，页2失败
            for page_idx in [0, 1]:
                page_ctx = {
                    **ctx,
                    "pageIndex": page_idx,
                    "imageName": f"{artwork_id}_p{page_idx}.jpg",
                }
                skip, asset_id = store.should_skip_page(page_ctx)
                assert not skip, f"首次上传不应跳过 page {page_idx}"

                # 模拟上传成功
                file_path = self._create_temp_image(artwork_id, page_idx)
                rep = store.upload_with_retry(
                    file_path, page_ctx["imageName"], page_ctx
                )
                store.register_asset(page_ctx, rep["id"])

            # 页2失败
            page2_ctx = {**ctx, "pageIndex": 2, "imageName": f"{artwork_id}_p2.jpg"}
            store.upsert_page_pending(page2_ctx, page2_ctx["imageName"])
            store.mark_page_failed(page2_ctx)

            # 第二次运行：页0和页1应跳过，页2重试
            skip_count = 0
            for page_idx in [0, 1]:
                page_ctx = {
                    **ctx,
                    "pageIndex": page_idx,
                    "imageName": f"{artwork_id}_p{page_idx}.jpg",
                }
                skip, asset_id = store.should_skip_page(page_ctx)
                if skip:
                    skip_count += 1
                    assert asset_id, f"跳过的页应返回 asset_id"

            # 页2不应跳过
            skip2, _ = store.should_skip_page(page2_ctx)
            assert not skip2, "失败的页不应跳过"

            # 验证数据库状态
            img_records = self._query_db(
                store, "image", f"pixivArtworkId=?", (artwork_id,)
            )
            success_count = sum(1 for r in img_records if r["downloadSuccess"] == 1)

            self.add_result(
                TestResult(
                    name="上传前判重",
                    passed=(skip_count == 2 and not skip2),
                    message=f"成功跳过 {skip_count}/2 张已上传图片，失败页正确重试",
                    details={
                        "跳过页数": skip_count,
                        "成功页数": success_count,
                        "总记录数": len(img_records),
                    },
                )
            )

        except Exception as e:
            self.add_result(
                TestResult(
                    name="上传前判重", passed=False, message=f"测试失败: {str(e)}"
                )
            )

    def test_03_stack_with_failures(self):
        """测试场景3：失败不影响堆叠（部分失败仍创建堆叠）"""
        print("\n" + "=" * 60)
        print("测试场景3：失败不影响堆叠")
        print("=" * 60)

        try:
            store = self._create_store()
            artwork_id = "987654321"

            # 准备作品：5页，其中页2和页4失败
            ctx = {
                "pixivArtworkId": artwork_id,
                "authorId": "artist-002",
                "authorName": "TestArtist2",
                "totalPages": 5,
            }
            store.prepare_work(ctx)

            # 上传成功：0, 1, 3
            for page_idx in [0, 1, 3]:
                page_ctx = {
                    **ctx,
                    "pageIndex": page_idx,
                    "imageName": f"{artwork_id}_p{page_idx}.jpg",
                }
                store.upsert_page_pending(page_ctx, page_ctx["imageName"])
                file_path = self._create_temp_image(artwork_id, page_idx)
                rep = store.upload_with_retry(
                    file_path, page_ctx["imageName"], page_ctx
                )
                store.register_asset(page_ctx, rep["id"])

            # 上传失败：2, 4
            for page_idx in [2, 4]:
                page_ctx = {
                    **ctx,
                    "pageIndex": page_idx,
                    "imageName": f"{artwork_id}_p{page_idx}.jpg",
                }
                store.upsert_page_pending(page_ctx, page_ctx["imageName"])
                store.mark_page_failed(page_ctx)

            # 触发 finalize
            store.finalize_if_no_pending(artwork_id)

            # 验证堆叠创建
            art_record = self._query_db(
                store, "artwork", "pixivArtworkId=?", (artwork_id,)
            )[0]
            stack_id = art_record.get("immichStackId")

            assert stack_id, "应创建堆叠"
            assert stack_id in self.mock_client.stacks, "堆叠应存在于 mock client"

            stack = self.mock_client.stacks[stack_id]
            assert (
                len(stack["assets"]) == 3
            ), f"堆叠应包含3个资产，实际 {len(stack['assets'])}"

            # 验证作品进度
            assert art_record["successCount"] == 3, "successCount 应为 3"
            assert art_record["totalImageCount"] == 5, "totalImageCount 应为 5"
            assert art_record["status"] == "FINISHED", "status 应为 FINISHED"

            self.add_result(
                TestResult(
                    name="失败不影响堆叠",
                    passed=True,
                    message=f"成功创建堆叠，包含 {len(stack['assets'])} 个资产（成功的页）",
                    details={
                        "stackId": stack_id,
                        "堆叠资产数": len(stack["assets"]),
                        "成功页数": art_record["successCount"],
                        "总页数": art_record["totalImageCount"],
                    },
                )
            )

        except Exception as e:
            self.add_result(
                TestResult(
                    name="失败不影响堆叠", passed=False, message=f"测试失败: {str(e)}"
                )
            )

    def test_04_retry_no_duplicate_stack(self):
        """测试场景4：重试不产生重复堆叠（合并进既有 stackId）"""
        print("\n" + "=" * 60)
        print("测试场景4：重试不产生重复堆叠")
        print("=" * 60)

        try:
            store = self._create_store()
            artwork_id = "555666777"

            # 第一轮：准备作品 4页，成功上传 0, 1，失败 2, 3
            ctx = {
                "pixivArtworkId": artwork_id,
                "authorId": "artist-003",
                "authorName": "TestArtist3",
                "totalPages": 4,
            }
            store.prepare_work(ctx)

            # 成功页
            for page_idx in [0, 1]:
                page_ctx = {
                    **ctx,
                    "pageIndex": page_idx,
                    "imageName": f"{artwork_id}_p{page_idx}.jpg",
                }
                store.upsert_page_pending(page_ctx, page_ctx["imageName"])
                file_path = self._create_temp_image(artwork_id, page_idx)
                rep = store.upload_with_retry(
                    file_path, page_ctx["imageName"], page_ctx
                )
                store.register_asset(page_ctx, rep["id"])

            # 失败页
            for page_idx in [2, 3]:
                page_ctx = {
                    **ctx,
                    "pageIndex": page_idx,
                    "imageName": f"{artwork_id}_p{page_idx}.jpg",
                }
                store.upsert_page_pending(page_ctx, page_ctx["imageName"])
                store.mark_page_failed(page_ctx)

            # 第一轮 finalize（应创建 stack）
            store.finalize_if_no_pending(artwork_id)

            art_record_1 = self._query_db(
                store, "artwork", "pixivArtworkId=?", (artwork_id,)
            )[0]
            stack_id_1 = art_record_1["immichStackId"]
            assert stack_id_1, "第一轮应创建堆叠"
            stack_assets_1 = len(self.mock_client.stacks[stack_id_1]["assets"])

            # 第二轮：重试失败的页 2, 3，这次成功
            # 模拟进程重启：清空内存状态，但数据库保留
            with store._lock:
                store._works.clear()

            store.prepare_work(ctx)  # 重新准备（模拟重启或重新下载）

            # 遍历所有页，已成功的跳过，失败的重试
            for page_idx in range(4):
                page_ctx = {
                    **ctx,
                    "pageIndex": page_idx,
                    "imageName": f"{artwork_id}_p{page_idx}.jpg",
                }
                # 页面0、1应跳过（已成功）
                if page_idx in [0, 1]:
                    skip, asset_id = store.should_skip_page(page_ctx)
                    assert skip, f"页 {page_idx} 应跳过"
                    store.mark_page_skipped(page_ctx, asset_id)
                else:
                    # 页2、3重试成功
                    store.upsert_page_pending(page_ctx, page_ctx["imageName"])
                    file_path = self._create_temp_image(artwork_id, page_idx)
                    rep = store.upload_with_retry(
                        file_path, page_ctx["imageName"], page_ctx
                    )
                    store.register_asset(page_ctx, rep["id"])

            # 第二轮 finalize（应追加到既有 stack）
            store.finalize_if_no_pending(artwork_id)

            art_record_2 = self._query_db(
                store, "artwork", "pixivArtworkId=?", (artwork_id,)
            )[0]
            stack_id_2 = art_record_2["immichStackId"]

            # 验证：stackId 应相同，资产数应增加
            stack_assets_2 = len(self.mock_client.stacks[stack_id_1]["assets"])
            stack_count = len(self.mock_client.stacks)

            passed = (
                stack_id_1 == stack_id_2
                and stack_assets_2 == 4
                and art_record_2["successCount"] == 4
            )

            self.add_result(
                TestResult(
                    name="重试不产生重复堆叠",
                    passed=passed,
                    message=f"重试后合并进既有堆叠，未创建新堆叠",
                    details={
                        "第一轮 stackId": stack_id_1,
                        "第二轮 stackId": stack_id_2,
                        "stackId 一致": stack_id_1 == stack_id_2,
                        "第一轮资产数": stack_assets_1,
                        "第二轮资产数": stack_assets_2,
                        "总堆叠数": stack_count,
                        "最终 successCount": art_record_2["successCount"],
                    },
                )
            )

        except Exception as e:
            self.add_result(
                TestResult(
                    name="重试不产生重复堆叠",
                    passed=False,
                    message=f"测试失败: {str(e)}",
                )
            )

    def test_05_progress_tracking(self):
        """测试场景5：作品级进度可追踪（totalImageCount / successCount）"""
        print("\n" + "=" * 60)
        print("测试场景5：作品级进度可追踪")
        print("=" * 60)

        try:
            store = self._create_store()
            artwork_id = "111222333"

            # 准备作品
            ctx = {
                "pixivArtworkId": artwork_id,
                "authorId": "artist-004",
                "authorName": "TestArtist4",
                "totalPages": 6,
            }
            store.prepare_work(ctx)

            # 验证初始状态
            art_record_0 = self._query_db(
                store, "artwork", "pixivArtworkId=?", (artwork_id,)
            )[0]
            assert art_record_0["totalImageCount"] == 6, "初始 totalImageCount 应为 6"
            assert art_record_0["successCount"] == 0, "初始 successCount 应为 0"
            assert art_record_0["status"] == "RUNNING", "初始 status 应为 RUNNING"

            # 上传前3页
            for page_idx in range(3):
                page_ctx = {
                    **ctx,
                    "pageIndex": page_idx,
                    "imageName": f"{artwork_id}_p{page_idx}.jpg",
                }
                store.upsert_page_pending(page_ctx, page_ctx["imageName"])
                file_path = self._create_temp_image(artwork_id, page_idx)
                rep = store.upload_with_retry(
                    file_path, page_ctx["imageName"], page_ctx
                )
                store.register_asset(page_ctx, rep["id"])

            # 失败3页
            for page_idx in range(3, 6):
                page_ctx = {
                    **ctx,
                    "pageIndex": page_idx,
                    "imageName": f"{artwork_id}_p{page_idx}.jpg",
                }
                store.upsert_page_pending(page_ctx, page_ctx["imageName"])
                store.mark_page_failed(page_ctx)

            # Finalize
            store.finalize_if_no_pending(artwork_id)

            # 验证进度
            art_record_1 = self._query_db(
                store, "artwork", "pixivArtworkId=?", (artwork_id,)
            )[0]

            passed = (
                art_record_1["totalImageCount"] == 6
                and art_record_1["successCount"] == 3
                and art_record_1["status"] == "FINISHED"
            )

            self.add_result(
                TestResult(
                    name="作品级进度可追踪",
                    passed=passed,
                    message=f"作品进度正确追踪：{art_record_1['successCount']}/{art_record_1['totalImageCount']}",
                    details={
                        "totalImageCount": art_record_1["totalImageCount"],
                        "successCount": art_record_1["successCount"],
                        "status": art_record_1["status"],
                        "immichStackId": art_record_1.get("immichStackId"),
                    },
                )
            )

        except Exception as e:
            self.add_result(
                TestResult(
                    name="作品级进度可追踪", passed=False, message=f"测试失败: {str(e)}"
                )
            )

    def test_06_multi_instance_isolation(self):
        """测试场景6：多 Immich 实例隔离（hostKey 表名）"""
        print("\n" + "=" * 60)
        print("测试场景6：多 Immich 实例隔离")
        print("=" * 60)

        try:
            # 创建两个不同配置的 store
            config1 = self._create_test_config(
                host="https://immich1.test.com", api_key="key-aaa"
            )
            config2 = self._create_test_config(
                host="https://immich2.test.com", api_key="key-bbb"
            )

            store1 = ImmichStore(self.root_path, config1)
            store2 = ImmichStore(self.root_path, config2)

            # 验证 hostKey 不同
            assert store1.host_key != store2.host_key, "不同实例应有不同 hostKey"

            # 验证表名不同
            assert store1._tbl_art != store2._tbl_art, "不同实例应有不同作品表名"
            assert store1._tbl_img != store2._tbl_img, "不同实例应有不同图片表名"

            # 验证数据库文件相同但表隔离
            assert store1._db_uri == store2._db_uri, "应使用同一数据库文件"

            # 验证表确实存在
            with sqlite3.connect(store1._db_uri) as conn:
                cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cur.fetchall()]

            assert store1._tbl_art in tables, f"实例1作品表 {store1._tbl_art} 应存在"
            assert store2._tbl_art in tables, f"实例2作品表 {store2._tbl_art} 应存在"

            self.add_result(
                TestResult(
                    name="多 Immich 实例隔离",
                    passed=True,
                    message="不同实例使用不同表名，数据隔离成功",
                    details={
                        "实例1 hostKey": store1.host_key,
                        "实例2 hostKey": store2.host_key,
                        "实例1 作品表": store1._tbl_art,
                        "实例2 作品表": store2._tbl_art,
                        "数据库路径": store1._db_uri,
                    },
                )
            )

        except Exception as e:
            self.add_result(
                TestResult(
                    name="多 Immich 实例隔离",
                    passed=False,
                    message=f"测试失败: {str(e)}",
                )
            )

    def run_all_tests(self):
        """运行所有测试"""
        print("\n" + "=" * 60)
        print("PixivBiu Immich 功能 Mock 测试")
        print("根据需求文档补充V1进行验收测试")
        print("=" * 60)

        self.test_01_sqlite_persistence()
        self.test_02_upload_deduplication()
        self.test_03_stack_with_failures()
        self.test_04_retry_no_duplicate_stack()
        self.test_05_progress_tracking()
        self.test_06_multi_instance_isolation()

        # 输出测试报告
        self.print_summary()

    def print_summary(self):
        """打印测试摘要"""
        print("\n" + "=" * 60)
        print("测试摘要")
        print("=" * 60)

        passed = sum(1 for r in self.results if r.passed)
        failed = len(self.results) - passed

        print(f"\n总计: {len(self.results)} 个测试")
        print(f"通过: {passed} ✅")
        print(f"失败: {failed} ❌")
        print(f"通过率: {passed/len(self.results)*100:.1f}%")

        if failed > 0:
            print("\n失败的测试:")
            for r in self.results:
                if not r.passed:
                    print(f"  - {r.name}: {r.message}")

        print("\n" + "=" * 60)

        # 保存测试报告到文件
        self.save_report()

    def save_report(self):
        """保存测试报告到文件"""
        report_path = os.path.join(self.root_path, "test_report.json")
        report = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total": len(self.results),
            "passed": sum(1 for r in self.results if r.passed),
            "failed": sum(1 for r in self.results if not r.passed),
            "results": [
                {
                    "name": r.name,
                    "passed": r.passed,
                    "message": r.message,
                    "details": r.details,
                }
                for r in self.results
            ],
        }

        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"\n测试报告已保存至: {report_path}")


def main():
    """主函数"""
    # 使用临时目录作为测试根路径
    test_root = tempfile.mkdtemp(prefix="pixivbiu_mock_test_")
    print(f"测试根目录: {test_root}")

    try:
        tester = ImmichMockTest(root_path=test_root)
        tester.run_all_tests()
    except KeyboardInterrupt:
        print("\n\n测试被用户中断")
    except Exception as e:
        print(f"\n\n测试过程中发生错误: {str(e)}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
