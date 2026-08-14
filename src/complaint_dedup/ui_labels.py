"""中文化界面展示标签，内部枚举值保持不变。"""

_LABELS = {
    "duplicate": "重复",
    "not_duplicate": "不重复",
    "review": "需人工复核",
    "pending": "待处理",
    "succeeded": "已完成",
    "failed": "失败",
    "queued": "排队中",
    "running": "运行中",
    "paused": "已暂停",
    "review_ready": "待人工复核",
    "completed_with_warnings": "已完成（有警告）",
    "reviewed": "已确认",
    "auto_merged": "自动合并",
    "confirmed": "人工确认",
    "singleton": "单例事件",
    "rejected": "已排除",
    "strict": "严格",
    "balanced": "平衡",
    "loose": "宽松",
    "extracting": "信息抽取",
    "generating_candidates": "生成候选对",
    "judging": "模型二审",
    "subject+exact_address": "主体＋精确地址",
    "subject+exact_address+issue": "主体＋精确地址＋核心问题",
    "subject+coarse_address+issue": "主体＋粗地址＋核心问题",
    "exact_address+issue": "精确地址＋核心问题",
    "issue": "核心问题",
    "hybrid_vector": "向量相似召回",
    "same_phone": "联系电话一致",
    "same_category": "事项分类一致",
    "different_subject": "主体不同",
    "different_branch": "分店不同",
    "different_address": "地址不同",
    "different_road": "道路不同",
    "different_house_no": "门牌号不同",
    "different_building": "楼栋不同",
    "different_shop_no": "铺位不同",
    "different_transaction": "交易不同",
    "different_primary_issue": "核心问题不同",
    "different_fact_chain": "事实链不同",
    "different_object": "对象不同",
    "different_request": "诉求不同",
    "referenced_work_order_mismatch": "引用工单不一致",
    "independent_issue": "存在独立问题",
    "uploaded": "等待后台处理",
    "parsing": "解析工单",
    "normalizing": "标准化词典",
    "reviewing": "待人工审核",
    "approval_requested": "正在发布词典",
    "commit_requested": "正在追加历史库",
    "committed": "已写入历史库",
    "bootstrap_history": "历史库冷启动",
    "bootstrap_compare": "首次 A/B 联合比对",
    "daily_increment": "每日新增",
    "correction": "补录或更正",
    "candidate": "待审核",
    "approved": "已通过",
    "uncertain": "存疑",
    "merged": "已合并",
    "exact": "精确命中",
    "fuzzy": "模糊命中",
    "unknown": "待识别",
    "manual_singleton": "保守单例",
}

_STAGE_LABELS = {
    "queued": "等待执行",
    "embedding": "向量化",
    "extracting": "信息抽取",
    "generating_candidates": "生成候选对",
    "judging": "模型二审",
    "clustering_events": "事件簇判定",
    "review": "人工复核",
    "failed": "执行失败",
    "uploaded": "等待后台处理",
    "parsing": "解析工单",
    "normalizing": "标准化词典",
    "reviewing": "待人工审核",
    "approval_requested": "正在发布词典",
    "commit_requested": "正在追加历史库",
    "committed": "已写入历史库",
}


def label(value: str | None) -> str:
    if not value:
        return ""
    if "," in value:
        parts = [part.strip() for part in value.split(",") if part.strip()]
        if parts and all(part in _LABELS for part in parts):
            return "、".join(_LABELS[part] for part in parts)
    return _LABELS.get(value, value)


def labels(values: list[str] | None) -> list[str]:
    return [label(value) for value in (values or [])]


def stage_label(value: str | None) -> str:
    if not value:
        return ""
    return _STAGE_LABELS.get(value, label(value))
