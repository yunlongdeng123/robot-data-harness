"""ADS quality_dashboard 入口（含 DWS → ADS 整链路）。

等价于 `robot-dh spark build-quality-ads`，留作 Notebook / IDE 单元跑用。
"""

from __future__ import annotations

import argparse
import json

from robot_dh.spark_jobs.quality_ads import build_quality_ads


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Spark local mode：构建 ADS quality_dashboard（含上游 DWS）。"
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    result = build_quality_ads(input_uri=args.input, output_uri=args.output, dt=args.date)
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
