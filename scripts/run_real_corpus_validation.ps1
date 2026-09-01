param(
    [ValidateSet("bootstrap_history", "bootstrap_compare", "daily_increment")]
    [string]$Mode = "bootstrap_history",
    [string]$BaseUrl = "http://127.0.0.1:8765",
    [string]$HistoryFile = "F:\Job Coding\重复筛选判定\半年以来历史表.xlsx",
    [string]$DailyFile = "F:\Job Coding\重复筛选判定\2026.8.12-12时-8.13-12时（当天）.xlsx",
    [string]$Name = "真实数据验证"
)

uv run python scripts/run_real_corpus_validation.py `
    --mode $Mode `
    --base-url $BaseUrl `
    --history $HistoryFile `
    --daily $DailyFile `
    --name $Name
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
