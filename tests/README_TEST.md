# Immich Mock 测试指南

## 概述

本测试脚本用于验证 PixivBiu Immich 存储模式的核心功能，基于《需求文档补充V1》中的验收标准。

## 测试环境

- **测试框架**: Python 标准库 (无需额外依赖)
- **Mock 客户端**: 模拟 Immich API 响应
- **测试数据**: 临时目录，测试后自动清理

## 运行测试

### 方式1：直接运行

```bash
cd /Users/ryan/DEV/Python/PixivBiu
python tests/test_immich_mock.py
```

### 方式2：使用 pytest（可选）

```bash
cd /Users/ryan/DEV/Python/PixivBiu
pytest tests/test_immich_mock.py -v
```

## 测试覆盖场景

### ✅ 场景1：SQLite 持久化生效

**验证内容:**
- 数据库文件创建 (`usr/cache/immich_record.db`)
- 作品表创建 (`artwork_upload_record_<hostKey>`)
- 图片表创建 (`artwork_image_record_<hostKey>`)
- 索引创建

**期望结果:**
- 数据库文件存在
- 表结构符合文档定义
- hostKey 正确生成

---

### ✅ 场景2：上传前判重

**验证内容:**
- 首次上传：所有页正常上传
- 重复执行：已成功页跳过，失败页重试
- `should_skip_page()` 正确返回跳过状态

**期望结果:**
- 已上传页返回 `(True, asset_id)`
- 失败页返回 `(False, None)`
- 数据库记录状态正确

---

### ✅ 场景3：失败不影响堆叠

**验证内容:**
- 多图作品部分页失败
- 任务结束仍创建堆叠
- 堆叠仅包含成功资产

**期望结果:**
- 即使有失败页，堆叠依然创建
- `immichStackId` 正确写入数据库
- `successCount` 反映实际成功数

---

### ✅ 场景4：重试不产生重复堆叠

**验证内容:**
- 第一轮：部分页成功，创建堆叠
- 第二轮：失败页重试成功
- 新资产追加到既有堆叠

**期望结果:**
- 两轮 `stackId` 相同
- 不创建新堆叠
- 堆叠资产数累加正确

---

### ✅ 场景5：作品级进度可追踪

**验证内容:**
- `totalImageCount` 正确记录总页数
- `successCount` 动态更新成功数
- `status` 正确反映状态 (RUNNING → FINISHED)

**期望结果:**
- 初始状态：`successCount=0, status=RUNNING`
- 任务结束：`successCount` 与实际一致，`status=FINISHED`

---

### ✅ 场景6：多 Immich 实例隔离

**验证内容:**
- 不同 `host/token` 生成不同 `hostKey`
- 表名包含 `hostKey` 后缀
- 使用同一数据库文件但表隔离

**期望结果:**
- 两个实例的表名不同
- 数据互不影响
- 切换实例不会误判已上传

---

## 测试输出

### 终端输出示例

```
============================================================
PixivBiu Immich 功能 Mock 测试
根据需求文档补充V1进行验收测试
============================================================

============================================================
测试场景1：SQLite 持久化生效
============================================================

✅ PASS: SQLite 持久化生效
   数据库文件、表、索引均创建成功
   - 数据库路径: /tmp/pixivbiu_test_xxxx/usr/cache/immich_record.db
   - 作品表名: artwork_upload_record_abc123456789
   - 图片表名: artwork_image_record_abc123456789
   - hostKey: abc123456789

...

============================================================
测试摘要
============================================================

总计: 6 个测试
通过: 6 ✅
失败: 0 ❌
通过率: 100.0%

============================================================

测试报告已保存至: /tmp/pixivbiu_test_xxxx/test_report.json
```

### 测试报告 JSON

测试结束后会生成 `test_report.json`，包含详细测试结果：

```json
{
  "timestamp": "2026-02-17 14:30:25",
  "total": 6,
  "passed": 6,
  "failed": 0,
  "results": [
    {
      "name": "SQLite 持久化生效",
      "passed": true,
      "message": "数据库文件、表、索引均创建成功",
      "details": {
        "数据库路径": "/tmp/pixivbiu_test_xxxx/usr/cache/immich_record.db",
        "作品表名": "artwork_upload_record_abc123456789",
        ...
      }
    },
    ...
  ]
}
```

## 故障排查

### 问题1：ImportError: No module named 'cust'

**解决方案:**
```bash
# 确保在项目根目录运行
cd /Users/ryan/DEV/Python/PixivBiu
python tests/test_immich_mock.py
```

### 问题2：数据库锁定

**解决方案:**
- 测试使用临时目录，每次运行独立
- 如遇锁定，检查是否有其他进程占用

### 问题3：测试失败

**排查步骤:**
1. 查看失败测试的详细消息
2. 检查 `test_report.json` 中的 `details` 字段
3. 查看数据库文件内容（SQLite Browser）

## 扩展测试

### 添加新测试场景

在 `ImmichMockTest` 类中添加新方法：

```python
def test_07_your_scenario(self):
    """测试场景7：你的测试描述"""
    print("\n" + "="*60)
    print("测试场景7：你的测试描述")
    print("="*60)
    
    try:
        # 测试代码
        store = self._create_store()
        # ...
        
        self.add_result(TestResult(
            name="你的测试名称",
            passed=True,
            message="测试通过",
            details={}
        ))
    except Exception as e:
        self.add_result(TestResult(
            name="你的测试名称",
            passed=False,
            message=f"测试失败: {str(e)}"
        ))
```

然后在 `run_all_tests()` 中调用：

```python
def run_all_tests(self):
    # ...
    self.test_07_your_scenario()
    # ...
```

## 验收标准对照表

| 需求文档验收标准      | 测试场景                         | 状态 |
| --------------------- | -------------------------------- | ---- |
| 1. SQLite 持久化生效  | test_01_sqlite_persistence       | ✅    |
| 2. 上传前判重         | test_02_upload_deduplication     | ✅    |
| 3. 失败不影响堆叠     | test_03_stack_with_failures      | ✅    |
| 4. 重试不产生重复堆叠 | test_04_retry_no_duplicate_stack | ✅    |
| 5. 作品级进度可追踪   | test_05_progress_tracking        | ✅    |
| 6. 多实例隔离（补充） | test_06_multi_instance_isolation | ✅    |

## 注意事项

1. **Mock 客户端限制**: 测试使用 mock 客户端，不会真正调用 Immich API
2. **临时文件**: 测试创建的临时文件会在测试结束后清理
3. **并发安全**: 当前测试为单线程，不测试并发场景
4. **真实环境**: 建议在测试环境的真实 Immich 实例上进行集成测试

## 下一步

- [ ] 补充读取接口测试（查询作品状态、进度）
- [ ] 补充并发测试（多线程同时上传）
- [ ] 补充异常恢复测试（数据库损坏、网络中断）
- [ ] 集成到 CI/CD 流程
