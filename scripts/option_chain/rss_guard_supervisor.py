#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

try:
    import psutil  # type: ignore
except Exception:
    psutil = None

_STOP = False


def _on_stop(_sig, _frame):
    global _STOP
    _STOP = True


def _utc_now() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat()


def _utc_id() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _sample_rss_mb(pid: int) -> float | None:
    if psutil is not None:
        try:
            proc = psutil.Process(pid)
            mem = proc.memory_info()
            return float(mem.rss) / (1024.0 * 1024.0)
        except Exception:
            return None
    try:
        out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True).strip()
        if not out:
            return None
        rss_kb = int(out.splitlines()[-1])
        return float(rss_kb) / 1024.0
    except Exception:
        return None


def _sample_rss_tree_mb(pid: int) -> float | None:
    if psutil is None:
        return _sample_rss_mb(pid)
    try:
        proc = psutil.Process(pid)
        procs = [proc, *proc.children(recursive=True)]
        total = 0
        for p in procs:
            try:
                if not p.is_running():
                    continue
                total += int(p.memory_info().rss)
            except Exception:
                continue
        return float(total) / (1024.0 * 1024.0)
    except Exception:
        return _sample_rss_mb(pid)


def _write_jsonl(fp: Path, row: dict[str, Any]) -> None:
    with fp.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _stop_child(proc: subprocess.Popen[Any], *, grace_s: float) -> int:
    if proc.poll() is not None:
        return int(proc.returncode or 0)
    proc.send_signal(signal.SIGINT)
    try:
        return int(proc.wait(timeout=max(0.5, grace_s)))
    except subprocess.TimeoutExpired:
        proc.terminate()
    try:
        return int(proc.wait(timeout=10.0))
    except subprocess.TimeoutExpired:
        proc.kill()
        return int(proc.wait(timeout=5.0))


def main() -> int:
    ap = argparse.ArgumentParser(description="RSS guardrail supervisor for option-chain scraper")
    ap.add_argument("--rss-max-mb", type=float, required=True, help="restart child when RSS exceeds this threshold")
    ap.add_argument("--tree-rss", action="store_true", help="monitor total RSS of child + descendants")
    ap.add_argument("--sample-interval-s", type=float, default=5.0)
    ap.add_argument("--cooldown-s", type=float, default=10.0)
    ap.add_argument("--graceful-stop-s", type=float, default=20.0)
    ap.add_argument("--max-restarts", type=int, default=0, help="0 means unlimited restarts")
    ap.add_argument("--run-root", default="artifacts/runs/option_chain_supervisor")
    ap.add_argument("cmd", nargs=argparse.REMAINDER, help="command to supervise; prefix with --")
    args = ap.parse_args()

    cmd = [c for c in args.cmd if c != "--"]
    if not cmd:
        raise SystemExit("no child command provided. Example: -- python apps/scrap/option_chain.py --currencies BTC,ETH")

    run_dir = Path(args.run_root).resolve() / _utc_id()
    run_dir.mkdir(parents=True, exist_ok=True)
    events_fp = run_dir / "supervisor_events.jsonl"
    stdout_fp = run_dir / "child_stdout.log"
    stderr_fp = run_dir / "child_stderr.log"
    pid_fp = run_dir / "child.pid"

    signal.signal(signal.SIGINT, _on_stop)
    signal.signal(signal.SIGTERM, _on_stop)

    restart_count = 0
    child: subprocess.Popen[Any] | None = None

    with stdout_fp.open("a", encoding="utf-8") as out, stderr_fp.open("a", encoding="utf-8") as err:
        while not _STOP:
            child = subprocess.Popen(cmd, stdout=out, stderr=err, text=True)
            pid_fp.write_text(str(child.pid), encoding="utf-8")
            _write_jsonl(
                events_fp,
                {
                    "ts_utc": _utc_now(),
                    "event": "child_start",
                    "pid": child.pid,
                    "restart_count": restart_count,
                    "rss_max_mb": float(args.rss_max_mb),
                    "cmd": cmd,
                },
            )
            print(f"[rss-supervisor] child started pid={child.pid} restarts={restart_count}")

            while not _STOP:
                rc = child.poll()
                if rc is not None:
                    _write_jsonl(
                        events_fp,
                        {
                            "ts_utc": _utc_now(),
                            "event": "child_exit",
                            "pid": child.pid,
                            "returncode": int(rc),
                            "restart_count": restart_count,
                        },
                    )
                    break

                rss_mb = _sample_rss_tree_mb(child.pid) if args.tree_rss else _sample_rss_mb(child.pid)
                if rss_mb is not None and rss_mb >= float(args.rss_max_mb):
                    _write_jsonl(
                        events_fp,
                        {
                            "ts_utc": _utc_now(),
                            "event": "rss_threshold",
                            "pid": child.pid,
                            "rss_mb": round(float(rss_mb), 3),
                            "tree_rss": bool(args.tree_rss),
                            "rss_max_mb": float(args.rss_max_mb),
                            "restart_count": restart_count,
                        },
                    )
                    print(
                        f"[rss-supervisor] rss threshold hit pid={child.pid} rss_mb={rss_mb:.1f} >= {float(args.rss_max_mb):.1f}"
                    )
                    _stop_child(child, grace_s=float(args.graceful_stop_s))
                    break

                _write_jsonl(
                    events_fp,
                    {
                        "ts_utc": _utc_now(),
                        "event": "sample",
                        "pid": child.pid,
                        "rss_mb": round(float(rss_mb), 3) if rss_mb is not None else None,
                        "tree_rss": bool(args.tree_rss),
                        "restart_count": restart_count,
                    },
                )
                time.sleep(max(0.2, float(args.sample_interval_s)))

            if _STOP:
                break

            restart_count += 1
            if int(args.max_restarts) > 0 and restart_count > int(args.max_restarts):
                _write_jsonl(
                    events_fp,
                    {
                        "ts_utc": _utc_now(),
                        "event": "max_restarts_exceeded",
                        "max_restarts": int(args.max_restarts),
                    },
                )
                break

            time.sleep(max(0.0, float(args.cooldown_s)))

    if child is not None and child.poll() is None:
        rc = _stop_child(child, grace_s=float(args.graceful_stop_s))
        _write_jsonl(
            events_fp,
            {
                "ts_utc": _utc_now(),
                "event": "supervisor_stop",
                "pid": child.pid,
                "returncode": int(rc),
                "restart_count": restart_count,
            },
        )

    print(f"[rss-supervisor] run_dir={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
