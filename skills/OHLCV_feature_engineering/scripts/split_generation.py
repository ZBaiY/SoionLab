from __future__ import annotations

import argparse
from pathlib import Path

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import dump_json, load_table


def run(input_path: Path, output_path: Path, *, train_bars: int, test_bars: int, step_bars: int, mode: str) -> dict:
    df = load_table(input_path)
    if "timestamp" not in df.columns:
        raise ValueError("Split generation requires a timestamp column.")
    timestamps = sorted(df["timestamp"].dropna().unique().tolist())
    splits = []
    train_start = 0
    while True:
        train_end = train_start + train_bars
        test_end = train_end + test_bars
        if test_end > len(timestamps):
            break
        splits.append({
            "train": {
                "start_ts": int(timestamps[0 if mode == "anchored" else train_start]),
                "end_ts": int(timestamps[train_end - 1]),
            },
            "test": {
                "start_ts": int(timestamps[train_end]),
                "end_ts": int(timestamps[test_end - 1]),
            },
        })
        train_start += step_bars
    result = {
        "mode": mode,
        "train_bars": train_bars,
        "test_bars": test_bars,
        "step_bars": step_bars,
        "split_count": len(splits),
        "splits": splits,
    }
    dump_json(output_path, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--train-bars", type=int, required=True)
    parser.add_argument("--test-bars", type=int, required=True)
    parser.add_argument("--step-bars", type=int, required=True)
    parser.add_argument("--mode", default="rolling")
    args = parser.parse_args()
    run(Path(args.input), Path(args.output), train_bars=args.train_bars, test_bars=args.test_bars, step_bars=args.step_bars, mode=args.mode)


if __name__ == "__main__":
    main()
