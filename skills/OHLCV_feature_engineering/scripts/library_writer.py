from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import shutil
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import LIBRARY_ROOT, dump_json, load_json, slugify, stable_hash, unique_path, utc_date_stamp, utc_now_iso


@contextlib.contextmanager
def _locked_index(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def run(
    parsed_path: Path,
    features_json_path: Path,
    features_yaml_path: Path,
    quality_md_path: Path,
    handoff_md_path: Path,
    output_path: Path,
    library_root: Path = LIBRARY_ROOT,
) -> dict[str, Any]:
    parsed_payload = load_json(parsed_path)
    features_payload = load_json(features_json_path)
    parsed = parsed_payload["parsed_directions"]
    specs = features_payload["feature_specifications"]

    dedupe_keys = [
        stable_hash({
            "family": spec["family"],
            "formula": spec["formula"]["expression"],
            "inputs": spec["formula"]["inputs"],
            "parameters": spec["formula"]["parameters"],
            "timing": spec["timing_semantics"],
        })
        for spec in specs
    ]

    index_path = library_root / "registry/index.json"
    lock_path = library_root / "registry/index.lock"

    with _locked_index(lock_path):
        if index_path.exists():
            index = json.loads(index_path.read_text(encoding="utf-8"))
        else:
            index = {"records": [], "dedupe_map": {}}

        related_records = sorted({
            index["dedupe_map"][key]["record_id"]
            for key in dedupe_keys
            if key in index.get("dedupe_map", {})
        })
        all_known = bool(dedupe_keys) and all(key in index.get("dedupe_map", {}) for key in dedupe_keys)

        family_slug = slugify(parsed.get("family_hint") or (sorted({spec["family"] for spec in specs})[0] if specs else "unknown_family"), max_len=24)
        objective_slug = slugify(parsed["interpreted_objective"], max_len=32)
        symbol_slug = slugify(parsed["input_domain_scope"].get("primary_symbol", "primary"), max_len=16)
        feature_slug = f"{family_slug}__{objective_slug}__{symbol_slug}"
        date_stamp = utc_date_stamp()
        spec_target = unique_path(library_root / "specs" / f"{feature_slug}__{parsed['observation_interval']}__{date_stamp}.yaml")
        report_target = unique_path(library_root / "reports" / f"{feature_slug}__{parsed['observation_interval']}__{date_stamp}.md")

        if not all_known:
            spec_target.parent.mkdir(parents=True, exist_ok=True)
            report_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(features_yaml_path, spec_target)
            merged_report = quality_md_path.read_text(encoding="utf-8").strip() + "\n\n" + handoff_md_path.read_text(encoding="utf-8").strip() + "\n"
            report_target.write_text(merged_report, encoding="utf-8")
        else:
            first = index["dedupe_map"][dedupe_keys[0]]
            spec_target = library_root / first["spec_path"]
            report_target = library_root / first["report_path"]

        record = {
            "record_id": f"ohlcvfe_{date_stamp}_{stable_hash({'direction_id': parsed['direction_id'], 'dedupe_keys': dedupe_keys})[:8]}",
            "created_from_direction_id": parsed["direction_id"],
            "timestamp_utc": utc_now_iso(),
            "repo_scope": "SoionLab",
            "domain": "ohlcv",
            "observation_interval": parsed["observation_interval"],
            "primary_family": sorted({spec["family"] for spec in specs}),
            "feature_ids": [spec["feature_id"] for spec in specs],
            "dedupe_keys": dedupe_keys,
            "related_records": related_records,
            "duplicate_of_existing_record": all_known,
            "files_to_write": {
                "registry_json": str(index_path.relative_to(library_root.parents[1])),
                "spec_yaml": str(spec_target.relative_to(library_root.parents[1])),
                "report_md": str(report_target.relative_to(library_root.parents[1])),
            },
        }

        index["records"].append({
            "record_id": record["record_id"],
            "direction_id": parsed["direction_id"],
            "spec_path": str(spec_target.relative_to(library_root)),
            "report_path": str(report_target.relative_to(library_root)),
            "dedupe_keys": dedupe_keys,
        })
        for key in dedupe_keys:
            index.setdefault("dedupe_map", {})[key] = {
                "record_id": record["record_id"],
                "spec_path": str(spec_target.relative_to(library_root)),
                "report_path": str(report_target.relative_to(library_root)),
            }

        dump_json(index_path, index)
        dump_json(output_path, record)
        return record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parsed", required=True)
    parser.add_argument("--features-json", required=True)
    parser.add_argument("--features-yaml", required=True)
    parser.add_argument("--quality-md", required=True)
    parser.add_argument("--handoff-md", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--library-root")
    args = parser.parse_args()
    run(
        Path(args.parsed),
        Path(args.features_json),
        Path(args.features_yaml),
        Path(args.quality_md),
        Path(args.handoff_md),
        Path(args.output),
        Path(args.library_root) if args.library_root else LIBRARY_ROOT,
    )


if __name__ == "__main__":
    main()
