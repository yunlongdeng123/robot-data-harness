#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from robot_dh.data.synthetic import generate_demo_dataset


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a robot-data-harness demo dataset")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration-sec", type=float, default=46.0)
    parser.add_argument("--fps", type=int, default=30)
    parser.add_argument("--num-buttons", type=int, default=5)
    parser.add_argument("--num-presses", type=int, default=25)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    generate_demo_dataset(
        output_dir=args.output,
        duration_sec=args.duration_sec,
        fps=args.fps,
        num_buttons=args.num_buttons,
        num_presses=args.num_presses,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
