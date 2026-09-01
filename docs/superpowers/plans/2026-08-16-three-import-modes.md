# 三种导入入口与活动历史库 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 将语料库工作台收敛为“历史库冷启动、首次联合比对、每日新增”三个入口，隐藏词典管理界面，并保证历史库冷启动成功后原子替换活动库、失败不破坏旧库。

**Architecture:** 保留现有 PostgreSQL/SQLite 兼容的 CorpusRepository、CorpusProcessor 和后台 worker。新增活动历史库 generation 指针与批次快照边界：冷启动先写入新的 generation，全部解析、归一化、事件构建成功后一次性切换指针；每日新增只读取当前活动 generation；首次联合比对在同一批次中先完成历史侧冷启动，再处理当天侧，提交前不让当天记录参与历史键构建。词典表继续作为后端内部实现，但所有词典页面、状态和导出路由从用户界面移除。

**Tech Stack:** FastAPI、Jinja2、HTMX、SQLAlchemy async、PostgreSQL/SQLite、Alembic、pytest、Docker Compose、SiliconFlow OpenAI-compatible API。

---

### Task 1: 建立失败测试基线

**Files:**
- Modify: `tests/test_corpus_web.py`
- Modify: `tests/test_corpus_pipeline.py`
- Modify: `tests/test_corpus_repository.py`
- Modify: `tests/test_corpus_worker.py`

- [ ] **Step 1: 增加三入口页面测试**

在 `test_corpus_web.py` 增加断言：主页包含“历史库冷启动”“首次 A/B 联合比对”“每日新增”，不包含“补录或更正”“词典审核”“词典状态”。

- [ ] **Step 2: 增加 correction 拒绝测试**

提交 `POST /batches`，表单 `mode=correction`，断言 HTTP 400，响应包含“批次类型无效”。

- [ ] **Step 3: 增加每日新增前置条件测试**

在没有活动历史库的临时数据库上提交 `daily_increment`，断言批次被拒绝或进入失败状态，并包含“尚未建立活动历史库”。

- [ ] **Step 4: 先运行新增测试确认失败**

运行：`uv run pytest tests/test_corpus_web.py tests/test_corpus_pipeline.py tests/test_corpus_repository.py tests/test_corpus_worker.py -q`

预期：新增断言失败，旧行为仍显示第四个入口或允许 correction；不得因为测试收集错误而失败。

### Task 2: 增加活动历史库 generation 与原子切换

**Files:**
- Modify: `src/complaint_dedup/corpus_schema.py`
- Modify: `src/complaint_dedup/corpus_repository.py`
- Create: `alembic/versions/20260816_0010_active_corpus_generation.py`
- Modify: `tests/test_corpus_schema.py`
- Modify: `tests/test_corpus_repository.py`

- [ ] **Step 1: 增加 schema 表和外键字段**

新增 `corpus_generations`：`id`、`generation_key`、`status`（building/active/failed/archived）、`source_batch_id`、`dictionary_version_id`、`created_at`、`activated_at`、`error_message`；在 `corpus_sources`、`corpus_records`、`events` 增加 `generation_id` 外键。为 `(generation_id, source_file_hash, source_row, row_hash)` 建唯一约束，为 `corpus_generations.status` 建索引。

- [ ] **Step 2: 写仓储接口测试**

测试 `create_generation()` 返回 building；`activate_generation(new_id)` 后新 generation 为 active、旧 generation 为 archived；`fail_generation()` 不改变旧 active generation；`active_generation()` 只返回 active 记录。

- [ ] **Step 3: 实现仓储接口**

在 `CorpusRepository` 中实现：

```python
async def active_generation(self) -> dict[str, Any] | None: ...
async def create_generation(self, batch_id: str, dictionary_version_id: int | None) -> int: ...
async def activate_generation(self, generation_id: int) -> None: ...
async def fail_generation(self, generation_id: int, message: str) -> None: ...
async def generation_for_batch(self, batch_id: str) -> dict[str, Any] | None: ...
```

所有写入新历史库的查询必须带 `generation_id`；每日查询默认使用 `active_generation()`，不再使用全局 `committed=true` 作为历史边界。

- [ ] **Step 4: 编写并运行 Alembic 迁移**

迁移同时兼容 PostgreSQL 和 SQLite 测试数据库；使用 `alembic upgrade head` 验证新表和字段存在。

- [ ] **Step 5: 运行仓储和 schema 测试确认通过**

运行：`uv run pytest tests/test_corpus_schema.py tests/test_corpus_repository.py -q`

### Task 3: 收敛流水线为三个业务模式

**Files:**
- Modify: `src/complaint_dedup/corpus_pipeline.py`
- Modify: `src/complaint_dedup/corpus_worker.py`
- Modify: `src/complaint_dedup/corpus_repository.py`
- Modify: `tests/test_corpus_pipeline.py`
- Modify: `tests/test_corpus_worker.py`

- [ ] **Step 1: 写失败测试**

覆盖以下行为：

1. `stage_records(..., batch_type="correction")` 抛出 `ValueError("批次类型无效")`。
2. bootstrap_history 创建 building generation，成功提交后 generation active。
3. 冷启动失败时旧 active generation 仍可查询，失败 generation 标为 failed。
4. bootstrap_compare 先处理 B，再处理 A；A 的未知锚点不能写入 B generation 的词典和事件键。
5. daily_increment 没有 active generation 时抛出 `ValueError("尚未建立活动历史库")`。

- [ ] **Step 2: 实现模式校验和 generation 绑定**

在 `CorpusProcessor.stage_records()` 开头仅允许 `bootstrap_history`、`bootstrap_compare`、`daily_increment`。bootstrap 模式创建或复用当前批次的 generation；daily 模式读取活动 generation，并将批次记录绑定到该 generation。删除 correction 的 source_type 分支和时间重叠特判。

- [ ] **Step 3: 实现 worker 的 bootstrap 原子流程**

worker 在 bootstrap_history 完成后执行词典内部生成、解析、事件构建，再调用 `activate_generation()`；任一步抛错调用 `fail_generation()`，不得把旧库标记失效。bootstrap_compare 在同一 worker 执行 B 完整构建并冻结，再处理 A，最后一次性提交当天结果；禁止创建用户不可见的隐藏 daily 批次。

- [ ] **Step 4: 实现 daily_increment 追加流程**

daily 只读取活动 generation 的已批准标准项和冻结事件，匹配完成后将新记录写入同一 generation；批次提交成功后更新事件成员和事件时间范围。

- [ ] **Step 5: 运行流水线与 worker 测试**

运行：`uv run pytest tests/test_corpus_pipeline.py tests/test_corpus_worker.py -q`

### Task 4: 前端只保留三个入口并隐藏词典

**Files:**
- Modify: `src/complaint_dedup/corpus_web.py`
- Modify: `templates/corpus_index.html`
- Modify: `templates/corpus_batch.html`
- Modify: `templates/partials/corpus_batch_progress.html`
- Modify: `templates/corpus_batches.html`
- Modify: `tests/test_corpus_web.py`

- [ ] **Step 1: 删除用户可达词典路由**

移除 `/batches/{batch_id}/dictionary`、词典审核、词典详情、批量通过和词典导出路由；旧 URL 返回 404。保留 repository 的词典方法供后台 pipeline 使用。

- [ ] **Step 2: 收敛 create_batch 校验**

只接受三个模式；历史库冷启动仅要求 `file_history`，首次联合比对要求两个文件，每日新增仅要求 `file_daily`。历史库冷启动成功后页面提示“已替换活动历史库”。

- [ ] **Step 3: 删除模板中的词典状态和第四入口**

主页只展示三种入口、最近批次和事件库统计；不渲染 `dictionary` 对象，不出现“词典”文字。批次详情只展示业务阶段、记录数、错误信息和操作按钮。

- [ ] **Step 4: 更新测试并运行**

运行：`uv run pytest tests/test_corpus_web.py -q`。确认页面三入口、correction 400、词典 URL 404、历史/每日文件控件随模式正确切换。

### Task 5: 配置 SiliconFlow 并补充真实运行脚本

**Files:**
- Modify: `.env.example`
- Modify: `README.md`
- Create: `scripts/run_real_corpus_validation.ps1`
- Modify: `tests/test_config.py`

- [ ] **Step 1: 配置默认键名**

在 `.env.example` 保留统一配置名并注明：`LLM_BASE_URL=https://api.siliconflow.cn/v1`、`LLM_MODEL=Qwen/Qwen3.6-27B`、`LLM_API_KEY` 只写本机/服务器 `.env`，不进入 Git；本地代码继续支持 `BASE_URL/API_KEY/MODEL_NAME` 别名映射。

- [ ] **Step 2: 增加真实数据脚本**

脚本接收历史表和当天表路径，调用现有 FastAPI API 创建 `bootstrap_history` 或 `bootstrap_compare`，轮询批次直到 `committed/failed`，输出 batch id、状态、记录数和结果导出路径。脚本不打印 API key 或投诉正文。

- [ ] **Step 3: 配置测试**

验证 SiliconFlow URL 可加载，未配置模型地址时启动校验失败，API key 不会出现在日志配置打印中。

### Task 6: 全量回归、真实数据和 Docker 部署

**Files:**
- Modify: `deploy/docker-compose.yml`
- Modify: `Dockerfile`
- Modify: `README.md`
- Add generated only under ignored `runtime/` and `output/`

- [ ] **Step 1: 运行全量测试**

运行：`uv run pytest -q`。预期所有已有测试与新增测试通过；若旧测试断言第四入口或词典页面，按新业务边界更新为三入口和后端隐藏。

- [ ] **Step 2: 本地真实数据验证**

使用：

```text
F:\Job Coding\重复筛选判定\半年以来历史表.xlsx
F:\Job Coding\重复筛选判定\2026.8.12-12时-8.13-12时（当天）.xlsx
```

先执行历史库冷启动并检查 active generation、事件数量和导出文件，再执行每日新增或首次联合比对，确认当天记录进入既有事件或新事件，数据库保留批次、成员关系和模型失败记录。

- [ ] **Step 3: 重新部署服务器**

在 `192.168.16.69:/data/caoke/complaint-dedup-validator` 更新代码；只在依赖或迁移变化时执行镜像构建，普通代码通过挂载目录更新后重启 API/worker。执行 `alembic upgrade head`，保留 PostgreSQL 备份，启动原端口 `28765`。

- [ ] **Step 4: 远端健康检查**

验证 `http://192.168.16.69:28765/` 返回 200；主页只有三个入口且无词典文案；创建一个小批次确认 worker 能领取并完成；检查 Docker 日志不包含 API key。

- [ ] **Step 5: 提交变更**

```bash
git add docs/superpowers/plans/2026-08-16-three-import-modes.md src templates tests alembic Dockerfile deploy README.md .env.example
git commit -m "feat: keep three corpus import modes and active history switching"
```

## 验收清单

- [ ] 用户入口只有历史库冷启动、首次联合比对、每日新增。
- [ ] correction 不能新建，旧 correction 记录仍可只读查询。
- [ ] 词典仅供后端使用，首页、批次详情、导航和导出均不展示词典。
- [ ] 冷启动成功才替换活动历史库，失败不影响旧库。
- [ ] 每日新增没有活动历史库时明确失败。
- [ ] bootstrap_compare 的 B 先建库，A 不污染 B 的标准项。
- [ ] SiliconFlow 配置从 `.env` 读取且密钥不进 Git、不进日志。
- [ ] 全量测试、真实数据任务、Docker 健康检查均有命令输出作为证据。
