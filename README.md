# 投诉全量工单比对系统

系统采用“全量工单库 + 时间窗口比对”架构：

1. 每次上传一份全量 Excel，系统按工单编号更新当前工单库；无编号时使用标准化复合指纹。
2. 新工单插入，已有工单覆盖最新字段；本次文件缺失的旧工单保留并标记为缺失。
3. 用户选择受理时间或办结时间，指定待比对时间段；被比对时间段默认取全量库中的补集。
4. 每次比对生成独立任务快照，可回看和导出，不覆盖历史结果。

系统不再使用账号隔离、每日新增表、历史代次滚动或后台批次队列。

## 本地启动

```powershell
uv sync
Copy-Item .env.example .env
uv run alembic upgrade head  # 仅适用于空库；旧库请先按下方说明重置
uv run uvicorn complaint_dedup.main:app --host 127.0.0.1 --port 8765
```

打开 `http://127.0.0.1:8765`。

## 时间窗口规则

- 默认比对字段：`受理时间`。
- 待比对时间段：用户选择；未填写时默认取上传文件中该字段的最大日期当天。
- 被比对时间段：默认是全量库中待比对时间段的补集。
- 手动指定被比对时间段时，必须与待比对时间段不重叠。
- 所选日期为空的工单进入待比对侧，并在任务详情中单独显示数量。
- 两侧联合判重，但禁止传递合并。

## 合并规则

- 普通工单使用“地区 + 街道 + 地点 + 事项”确定性事件键。
- 食品安全、欠薪、产品质量三类企业问题使用“地区 + 街道 + 企业主体 + 问题族”事件键。
- 不同街道、不同问题族不合并。
- 无法提取高置信企业主体时保持单例，不因关键词相同而合并。
- 人工禁止关系跨任务长期生效，但不会自动改写已生成的历史任务。

## 测试

```powershell
uv run pytest -q
```

当前测试覆盖解析、标准化、全量同步、窗口补集、空日期、任务快照、企业主体合并、人工禁止关系、授权和 Web 上传。

## Docker 部署

内网部署使用 `deploy/compose.intranet.yaml`，包含 PostgreSQL、API 和 worker，默认端口 `28765`。数据库表会在 API 启动时自动创建并校验，实施人员只需配置模型连接：

```bash
mkdir -p config
cp deploy/env.intranet.example config/.env
# 修改 config/.env 中的 LLM_BASE_URL、LLM_API_KEY、LLM_MODEL
docker compose --project-directory . -f deploy/compose.intranet.yaml up -d
```

当前版本删除了账号隔离、历史代次和旧批次模型。正常 Docker 启动会自动创建并校验当前表；只有明确需要破坏性清空旧库时，才执行：

```powershell
uv run python scripts/reset_database.py --confirm-reset
```

该命令会删除旧表和旧 `alembic_version`，然后初始化当前使用的 9 张表（含 `license_state`）。生产环境必须先停止服务并完成数据库备份。

生产凭据只保存在服务器 `config/.env`，不得提交 Git。

纯内网离线构建、镜像导出、授权续期和回滚步骤见[《内网离线交付与续期部署手册》](docs/内网离线交付与续期部署手册.md)。
