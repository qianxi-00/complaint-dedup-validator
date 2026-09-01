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
- 重新执行 `历史库冷启动` 会建立新的历史代次并替换旧代次。

上传、解析、词典发布和增量提交均由后台 worker 异步执行。API 只保存文件并创建任务，页面通过 HTMX 轮询进度。

## 技术栈

- Python 3.12、uv
- FastAPI、Jinja2、HTMX
- SQLAlchemy Async、PostgreSQL/asyncpg
- SQLite/aiosqlite 仅用于本地开发和自动化测试
- XlsxWriter 导出、openpyxl 验证

主流程以规则和 PostgreSQL 精确键为快速通道；对无法精确归一化的锚点、事项批量调用 `.env` 配置的大模型作存疑判定，低置信度仍保守保留为单例。主流程不依赖 Milvus、Embedding 或 Rerank。

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

## 词典

词典只在后台数据库中维护，不在前端展示。历史冷启动生成并发布标准街道、锚点和事项；每日新增不会直接污染已发布词典，模型无法高置信归一化的内容保守保存为单例事件。

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
