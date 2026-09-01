from __future__ import annotations

import argparse
import time
from pathlib import Path

import httpx


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=("bootstrap_history", "bootstrap_compare", "daily_increment"),
        default="bootstrap_history",
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8765")
    parser.add_argument(
        "--history",
        default=r"F:\Job Coding\重复筛选判定\半年以来历史表.xlsx",
    )
    parser.add_argument(
        "--daily",
        default=r"F:\Job Coding\重复筛选判定\2026.8.12-12时-8.13-12时（当天）.xlsx",
    )
    parser.add_argument("--name", default="真实数据验证")
    parser.add_argument("--timeout", type=float, default=12 * 60 * 60)
    args = parser.parse_args()

    history = Path(args.history)
    daily = Path(args.daily)
    if args.mode in {"bootstrap_history", "bootstrap_compare"} and not history.is_file():
        raise SystemExit(f"历史文件不存在：{history}")
    if args.mode in {"bootstrap_compare", "daily_increment"} and not daily.is_file():
        raise SystemExit(f"当天文件不存在：{daily}")

    data = {"mode": args.mode, "name": args.name}
    files = {}
    handles = []
    try:
        if args.mode in {"bootstrap_history", "bootstrap_compare"}:
            handle = history.open("rb")
            handles.append(handle)
            files["file_history"] = (history.name, handle, "application/octet-stream")
        if args.mode in {"bootstrap_compare", "daily_increment"}:
            handle = daily.open("rb")
            handles.append(handle)
            files["file_daily"] = (daily.name, handle, "application/octet-stream")

        with httpx.Client(base_url=args.base_url.rstrip("/"), timeout=120.0) as client:
            created = client.post("/batches", data=data, files=files, follow_redirects=False)
            if created.status_code != 303:
                raise SystemExit(
                    f"创建批次失败，HTTP {created.status_code}：{created.text}"
                )
            location = created.headers.get("location")
            if not location:
                raise SystemExit("创建批次成功但未返回批次地址")
            batch_url = location if location.startswith("http") else f"{args.base_url.rstrip('/')}{location}"
            batch_id = batch_url.rstrip("/").split("/")[-1]
            print(f"已创建批次 {batch_id}，开始等待后台处理。", flush=True)

            deadline = time.monotonic() + args.timeout
            while True:
                batch = client.get(f"{batch_url}/data").json()
                print(
                    "状态={status} 阶段={stage} 工单数={total_records} 最近操作={updated_at}".format(
                        **batch
                    ),
                    flush=True,
                )
                if batch.get("status") in {"committed", "failed"}:
                    break
                if time.monotonic() >= deadline:
                    raise SystemExit("批次等待超时")
                time.sleep(5)
            if batch.get("status") != "committed":
                raise SystemExit(f"批次未成功完成：{batch.get('error_message')}")

            output = Path("runtime/corpus_results") / f"真实数据验证_{batch_id}.xlsx"
            output.parent.mkdir(parents=True, exist_ok=True)
            exported = client.get("/exports/corpus")
            exported.raise_for_status()
            output.write_bytes(exported.content)
            print(f"批次已完成：{batch_id}", flush=True)
            print(f"结果文件：{output}", flush=True)
    finally:
        for handle in handles:
            handle.close()


if __name__ == "__main__":
    main()
