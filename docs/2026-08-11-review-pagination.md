# Review Pagination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复召回依据筛选，并将候选对和已合并事件组分别改为每页 10 条的 HTMX 分页。

**Architecture:** PostgreSQL 查询层负责一致的筛选、计数和分页；FastAPI 路由只组装分页上下文；Jinja2 partial 分别渲染候选对和事件组，两个分页互不影响。

**Tech Stack:** Python 3.12、FastAPI、SQLAlchemy Async、Jinja2、HTMX、pytest。

---

### Task 1: 候选筛选与分页

**Files:**
- Modify: `templates/partials/pairs.html`
- Modify: `src/complaint_dedup/async_web.py`
- Test: `tests/test_async_web.py`

- [ ] 写失败测试，验证召回依据中文下拉、重置按钮、结果总数和每页 10 条。
- [ ] 运行定向测试并确认旧实现失败。
- [ ] 将候选路由和初始任务页的分页大小统一为 10。
- [ ] 修改模板并保留翻页查询参数。
- [ ] 运行定向测试确认通过。

### Task 2: 已合并事件组分页

**Files:**
- Modify: `src/complaint_dedup/async_database.py`
- Modify: `src/complaint_dedup/async_web.py`
- Create: `templates/partials/event_groups.html`
- Modify: `templates/job.html`
- Test: `tests/test_async_database.py`
- Test: `tests/test_async_web.py`

- [ ] 写失败测试，验证默认只查询成员数大于 1 的事件组。
- [ ] 运行定向测试并确认旧实现失败。
- [ ] 增加事件组分页查询与计数方法。
- [ ] 新增独立 HTMX 事件组路由和 partial。
- [ ] 运行定向测试确认通过。

### Task 3: 验证与部署

**Files:**
- Modify: `static/app.css`（仅在分页布局需要时）

- [ ] 运行完整 pytest。
- [ ] 同步代码并重启 API。
- [ ] 使用真实浏览器验证筛选、重置、候选翻页和事件组翻页。
- [ ] 检查桌面及移动端页面无整体横向溢出。
