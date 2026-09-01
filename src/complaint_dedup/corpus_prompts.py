from __future__ import annotations

import json
from collections.abc import Sequence


NORMALIZATION_PROMPT_VERSION = "corpus-normalization-v1"


def build_normalization_messages(
    records: Sequence[dict],
) -> list[dict[str, str]]:
    system = (
        "你是投诉语料库归一化审核器。只能依据输入工单原文和候选标准项作判断，不能猜测。"
        "候选项的 id 只能原样选择，不能新增、改写或编造 id。"
        "锚点必须区分主体、地标、道路、门牌、楼栋和方向；同名不同地点不得合并。"
        "事项必须按核心事实和诉求判断，不能仅因事项分类相同就合并。"
        "如果没有足够证据，anchor_id 或 issue_id 返回 null，置信度返回 0。"
        "只输出 JSON：{\"decisions\":[{\"record_id\":\"...\",\"anchor_id\":null,"
        "\"issue_id\":null,\"anchor_confidence\":0,\"issue_confidence\":0,\"reason\":\"...\"}]}。"
    )
    payload = json.dumps(list(records), ensure_ascii=False, separators=(",", ":"))
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": payload},
    ]
