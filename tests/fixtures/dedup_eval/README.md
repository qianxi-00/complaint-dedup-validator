# 判重评估冻结集

由 `scripts/build_eval_set.py` 从本地全量库分层抽样生成；文本已掩码姓名/手机号/身份证，保留工单编号与订单号等案件标识。

- 生成时间：2026-09-10T10:07:15.545557+00:00
- 工单数：1118，标注对：573，黄金事件簇：141
- 标签分布：{"same": 339, "uncertain": 341, "different": 234}
- 划分：{"train": 362, "dev": 179, "test": 373}

字段说明：
- `records.jsonl`：record_key/work_order_id/title/appeal_text/category/received_at/strata 等；
- `pairs.jsonl`：left/right/label(same|different)/reason_code/label_source/split；
- `clusters.jsonl`：黄金事件成员。

运行评估：`uv run python scripts/eval_dedup.py --dataset tests/fixtures/dedup_eval`
