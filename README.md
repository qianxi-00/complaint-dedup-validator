# 投诉事件归一化与判重系统

系统采用“持续语料库 + 标准街道/锚点/问题词典 + 精确事件键”的增量架构。历史工单只在首次冷启动时导入；之后每天上传新增工单，审核后追加到冻结历史库。

核心事件键为：

```text
street_id + anchor_id + issue_id + event_key_version
```

系统不使用相似关系的传递合并，不会因为“工单 A 像 B、B 像 C”就把三者滚成一个大簇。未通过词典审核或无法归一化的工单会保守保留为单例事件。

## 业务模式

- `历史库冷启动`：上传历史文件 B，抽取候选词典，审核后建立冻结事件库。
- `首次 A/B 联合比对`：先处理并冻结 B，随后把当天文件 A 排入每日增量。
- `每日新增`：历史库存在后只上传当天文件 A。
- `补录或更正`：允许导入与历史时间范围重叠的数据。

上传、解析、词典发布和增量提交均由后台 worker 异步执行。API 只保存文件并创建任务，页面通过 HTMX 轮询进度。

## 技术栈

- Python 3.12、uv
- FastAPI、Jinja2、HTMX
- SQLAlchemy Async、PostgreSQL/asyncpg
- SQLite/aiosqlite 仅用于本地开发和自动化测试
- XlsxWriter 导出、openpyxl 验证

主流程不依赖 Milvus、Embedding、Rerank 或 LLM。后续若 PostgreSQL `pg_trgm` 对未知标准项召回不足，只对标准锚点和标准问题增加 pgvector/模型辅助，不向量化全部投诉原文。

## 本地启动

```powershell
uv sync
Copy-Item .env.example .env
uv run alembic upgrade head
uv run uvicorn complaint_dedup.main:app --host 127.0.0.1 --port 8765
```

SQLite 模式会在 API 进程内启动轻量 worker。PostgreSQL 部署应另开进程：

```powershell
uv run python -m complaint_dedup.worker_main
```

打开 `http://127.0.0.1:8765`。

## 词典审核

历史解析后进入词典审核工作台，分为：

- 标准街道
- 地点/主体锚点
- 核心问题

每项展示证据数、别名数和审核状态，支持通过、拒绝、存疑、改名、合并和按别名拆分。可以批量通过达到证据阈值且无跨街道同名风险的候选。候选和存疑项不会进入正式匹配。

已发布词典不可被每日未知项直接修改。新增未知内容进入独立的 `delta-<batch_id>` 候选版本，并以单例事件保存，避免污染正式词典。

## 导出

全量导出包含两个工作表：

- `重复项`
- `孤立工单`

第一列为事件名称，后续列保持历史表原始业务字段及顺序。同事件行相邻，并按事件交替使用浅蓝 `#EAF2FB` 和浅米 `#FFF8E7`；同时保留冻结首行、自动筛选、自动换行和公式注入防护。

## 测试

```powershell
uv run pytest -q
```

测试覆盖解析、词典状态、事件键、异步租约、上传队列、人工剔除、导出完整性和 Alembic 迁移。

## Docker 部署

`deploy/compose.yaml` 将代码、配置、运行数据和依赖镜像分离：

```text
complaint-dedup-validator/
├── image/Dockerfile
├── app/
├── config/.env
├── runtime/
└── compose.yaml
```

首次或依赖变化时重建镜像；普通代码更新只同步 `app/` 并重启容器。部署顺序：

```bash
docker compose run --rm migrate
docker compose up -d api worker
```

默认对外端口为 `28765`。生产环境凭据只写入服务器 `config/.env`，不得提交 Git。
