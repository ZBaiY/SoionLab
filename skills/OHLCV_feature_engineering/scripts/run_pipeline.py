from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import dump_json
from direction_parsing import run as run_direction_parsing
from feature_synthesis import run as run_feature_synthesis
from timing_audit import run as run_timing_audit
from family_classification import run as run_family_classification
from quality_evaluation import run as run_quality_evaluation
from handoff_builder import run as run_handoff_builder
from writer_guideline_builder import run as run_writer_guideline_builder
from library_writer import run as run_library_writer


def run_pipeline(input_path: Path, output_dir: Path) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    parsed_path = output_dir / "parsed_directions.json"
    candidate_path = output_dir / "candidate_features.json"
    audited_path = output_dir / "audited_features.json"
    classified_path = output_dir / "classified_features.json"
    feature_yaml_path = output_dir / "feature_specs.yaml"
    quality_json_path = output_dir / "quality_report.json"
    quality_md_path = output_dir / "quality_report.md"
    handoff_md_path = output_dir / "implementation_handoff.md"
    writer_guidelines_md_path = output_dir / "writer_guidelines.md"
    library_record_path = output_dir / "library_record.json"

    run_direction_parsing(input_path, parsed_path)
    run_feature_synthesis(parsed_path, candidate_path)
    run_timing_audit(candidate_path, audited_path)
    run_family_classification(audited_path, classified_path, feature_yaml_path)
    run_quality_evaluation(classified_path, quality_json_path, quality_md_path)
    run_handoff_builder(classified_path, quality_json_path, handoff_md_path)
    run_writer_guideline_builder(classified_path, quality_json_path, writer_guidelines_md_path)
    run_library_writer(parsed_path, classified_path, feature_yaml_path, quality_md_path, handoff_md_path, writer_guidelines_md_path, library_record_path)

    manifest = {
        "parsed_directions.json": str(parsed_path),
        "feature_specs.yaml": str(feature_yaml_path),
        "quality_report.md": str(quality_md_path),
        "implementation_handoff.md": str(handoff_md_path),
        "writer_guidelines.md": str(writer_guidelines_md_path),
        "library_record.json": str(library_record_path),
    }
    dump_json(output_dir / "pipeline_manifest.json", manifest)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    run_pipeline(Path(args.input), Path(args.output_dir))


if __name__ == "__main__":
    main()
