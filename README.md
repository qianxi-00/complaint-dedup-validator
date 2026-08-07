# Complaint Dedup Validator

本项目是一个本机单用户的投诉重复项验证台。它只比较文件 A 与文件 B，不执行表内去重；大模型负责投诉结构化抽取和候选对二审，程序负责文件处理、候选索引、任务恢复、人工确认和 Excel 导出。

## 快速启动

需要 Python 3.12 和 [uv](https://docs.astral.sh/uv/)：

```powershell
uv sync
Copy-Item .env.example .env
# 编辑 .env，至少填写 LLM_MODEL；LLM_BASE_URL 指向本地 OpenAI 兼容接口
uv run uvicorn complaint_dedup.main:app --host 127.0.0.1 --port 8765
```

也可以运行 `./start.ps1`，脚本会同步环境、启动服务并打开浏览器。

打开 `http://127.0.0.1:8765`，依次上传 A/B 文件、选择工作表和字段映射、创建任务，在候选对页面逐条确认或否决，最后导出 XLSX。

## 模型接口

默认使用 `LLM_BASE_URL` 下的 `/chat/completions`（即完整地址通常为 `http://127.0.0.1:8000/v1/chat/completions`）。请求采用 OpenAI 兼容格式，不要求服务支持 JSON Schema。模型名、超时、重试、批次大小和并发等配置全部写入 `.env`，由 `src/complaint_dedup/config.py` 统一加载。模型状态页提供连通测试。

## 输入与输出

输入支持 `.xlsx`、`.xls` 和 UTF-8/GB18030 `.csv`。字段可自动识别，也可以在上传后手工映射：工单编号、受理时间、诉求标题、事项分类、市民诉求。A/B 合计默认最多 10,000 行，多工作表按最终选择的工作表计数。

输出文件包含五个工作表：`结果总览`、`候选对`、`事件组`、`抽取失败`、`模型失败`。只有人工确认“重复”的候选关系才会进入事件组。

## 任务与恢复

任务元数据、原始字段和模型批次状态保存在 SQLite；上传文件和导出结果位于 `runtime/`。程序重启后，未完成的运行批次会回到队列，已经成功的记录不会再次请求模型。暂停/继续通过任务页面手动操作。

## 公开仓库安全边界

`.env`、原始 Word/Excel/CSV、运行数据库、日志和真实模型响应均被 `.gitignore` 排除。公开仓库只应提交脱敏夹具和代码，不要把真实投诉数据复制到测试目录。

## 测试

```powershell
uv run pytest -q
```
