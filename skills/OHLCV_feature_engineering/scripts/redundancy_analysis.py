from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import (
    dump_json,
    load_json,
    load_table,
    mechanism_class_from_label,
    parse_features_arg,
)


def _connected_components(nodes: list[str], edges: list[tuple[str, str]]) -> list[list[str]]:
    adjacency = {node: set() for node in nodes}
    for left, right in edges:
        adjacency[left].add(right)
        adjacency[right].add(left)

    seen: set[str] = set()
    components: list[list[str]] = []
    for node in nodes:
        if node in seen:
            continue
        stack = [node]
        component: list[str] = []
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            component.append(current)
            stack.extend(sorted(adjacency[current] - seen))
        components.append(sorted(component))
    return components


def _quality_map(quality_input: Path | None) -> dict[str, dict[str, Any]]:
    if quality_input is None:
        return {}
    payload = load_json(quality_input)
    rows = payload.get("feature_evaluations", [])
    return {row["feature"]: row for row in rows}


def _evidence_score(quality: dict[str, Any]) -> tuple[float, float, float]:
    decision_score = float(quality.get("decision_score", {}).get("raw", 0.0))
    metrics = quality.get("metrics", {})
    significance = max((-math.log10(max(float(metric.get("p_value", 1.0)), 1e-12)) for metric in metrics.values()), default=0.0)
    baseline_lift = max((float(metric.get("lift_vs_simple", 0.0)) for metric in metrics.values()), default=0.0)
    return decision_score, significance, baseline_lift


def _pick_representative(component: list[str], corr: pd.DataFrame, quality_rows: dict[str, dict[str, Any]]) -> tuple[str, list[str]]:
    def score(feature: str) -> tuple[float, float, float, str]:
        quality = quality_rows.get(feature, {})
        decision_score, significance, baseline_lift = _evidence_score(quality)
        mean_abs_corr = 0.0
        if len(component) > 1:
            values = [abs(float(corr.loc[feature, other])) for other in component if other != feature and pd.notna(corr.loc[feature, other])]
            mean_abs_corr = sum(values) / len(values) if values else 0.0
        return (decision_score, significance, baseline_lift, feature if mean_abs_corr == 0 else "")

    representative = max(component, key=score)
    deferred = [feature for feature in component if feature != representative]
    return representative, deferred


def _select_by_mechanism(component: list[str], quality_rows: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    grouped: dict[str, list[str]] = {}
    for feature in component:
        quality = quality_rows.get(feature, {})
        mechanism = mechanism_class_from_label(feature, quality.get("family"))
        grouped.setdefault(mechanism, []).append(feature)

    kept: list[dict[str, Any]] = []
    archived: list[str] = []
    for mechanism, members in sorted(grouped.items()):
        ranked = sorted(
            members,
            key=lambda feature: _evidence_score(quality_rows.get(feature, {})),
            reverse=True,
        )
        representative = ranked[0]
        kept.append({
            "mechanism_class": mechanism,
            "representative_feature": representative,
            "members": ranked,
        })
        archived.extend(ranked[1:])
    return kept, archived


def _temporal_override_allowed(feature: str, quality_rows: dict[str, dict[str, Any]], selected_non_temporal: list[str]) -> bool:
    quality = quality_rows.get(feature, {})
    decision = quality.get("decision")
    if decision != "keep":
        return False

    feature_score = _evidence_score(quality)
    non_temporal_scores = [
        _evidence_score(quality_rows[item])
        for item in selected_non_temporal
        if quality_rows.get(item, {}).get("feature_role") != "temporal_feature"
    ]
    if not non_temporal_scores:
        return False

    max_significance = max(score[1] for score in non_temporal_scores)
    max_lift = max(score[2] for score in non_temporal_scores)
    return feature_score[1] > max_significance and feature_score[2] > max_lift


def _build_output_sets(clusters: list[dict[str, Any]], soft_groups: list[dict[str, Any]],
                       quality_rows: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    hard_archived = {feature for cluster in clusters for feature in cluster["deferred_features"]}
    hard_kept = {cluster["representative_feature"] for cluster in clusters}

    selected_by_soft_group: set[str] = set()
    research_only: set[str] = set()
    for group in soft_groups:
        for item in group["mechanism_representatives"]:
            selected_by_soft_group.add(item["representative_feature"])
        research_only.update(group["mechanism_archived_features"])

    implement_first: list[dict[str, Any]] = []
    selected_non_temporal: list[str] = []
    for feature in sorted(selected_by_soft_group):
        quality = quality_rows.get(feature, {})
        role = quality.get("feature_role")
        if feature in hard_archived:
            continue
        if role == "temporal_feature":
            continue
        if quality.get("decision") == "keep":
            implement_first.append({
                "feature": feature,
                "family": quality.get("family"),
                "feature_role": role,
                "mechanism_class": mechanism_class_from_label(feature, quality.get("family")),
                "selection_reason": "hard-duplicate safe and selected as best representative within its mechanism class",
            })
            selected_non_temporal.append(feature)
        else:
            research_only.add(feature)

    for feature in sorted(selected_by_soft_group):
        quality = quality_rows.get(feature, {})
        role = quality.get("feature_role")
        if feature in hard_archived or role != "temporal_feature":
            continue
        if _temporal_override_allowed(feature, quality_rows, selected_non_temporal):
            implement_first.append({
                "feature": feature,
                "family": quality.get("family"),
                "feature_role": role,
                "mechanism_class": mechanism_class_from_label(feature, quality.get("family")),
                "selection_reason": "temporal override passed non-temporal significance and baseline-lift gate",
            })
        else:
            research_only.add(feature)

    archive_only = sorted(hard_archived | {feature for feature in research_only if feature in hard_archived})
    research_only = sorted((selected_by_soft_group - {item["feature"] for item in implement_first}) | research_only - hard_archived)

    return {
        "implement_first": implement_first,
        "research_only": [
            {
                "feature": feature,
                "family": quality_rows.get(feature, {}).get("family"),
                "feature_role": quality_rows.get(feature, {}).get("feature_role"),
                "mechanism_class": mechanism_class_from_label(feature, quality_rows.get(feature, {}).get("family")),
                "selection_reason": (
                    "temporal defaulted to research-only" if quality_rows.get(feature, {}).get("feature_role") == "temporal_feature"
                    else "retained as a distinct mechanism representative but not selected for immediate implementation"
                ),
            }
            for feature in research_only
        ],
        "archive_only": [
            {
                "feature": feature,
                "family": quality_rows.get(feature, {}).get("family"),
                "feature_role": quality_rows.get(feature, {}).get("feature_role"),
                "mechanism_class": mechanism_class_from_label(feature, quality_rows.get(feature, {}).get("family")),
                "selection_reason": "hard duplicate or same-mechanism soft-group duplicate",
            }
            for feature in archive_only
        ],
    }


def run(input_path: Path, output_path: Path, *, features: str | list[str] | None,
        threshold: float = 0.85, quality_input: Path | None = None, soft_threshold: float = 0.50) -> dict:
    df = load_table(input_path)
    feature_cols = parse_features_arg(features, df)
    corr = df[feature_cols].corr(method="spearman")
    pairs = []
    graph_edges: list[tuple[str, str]] = []
    soft_pairs = []
    soft_edges: list[tuple[str, str]] = []
    for i, left in enumerate(feature_cols):
        for right in feature_cols[i + 1:]:
            value = corr.loc[left, right]
            if pd.isna(value):
                continue
            abs_value = abs(float(value))
            if abs_value >= threshold:
                graph_edges.append((left, right))
                pairs.append({"left": left, "right": right, "spearman_corr": float(value)})
            if abs_value >= soft_threshold:
                soft_edges.append((left, right))
                soft_pairs.append({"left": left, "right": right, "spearman_corr": float(value)})

    quality_rows = _quality_map(quality_input)
    components = _connected_components(feature_cols, graph_edges)
    soft_components = _connected_components(feature_cols, soft_edges)
    clusters = []
    minimal_basis = []
    for index, component in enumerate(components, start=1):
        representative, deferred = _pick_representative(component, corr, quality_rows)
        clusters.append({
            "cluster_id": f"C{index:02d}",
            "members": component,
            "representative_feature": representative,
            "deferred_features": deferred,
        })
        minimal_basis.append(representative)

    soft_groups = []
    for index, component in enumerate(soft_components, start=1):
        mechanism_representatives, mechanism_archived = _select_by_mechanism(component, quality_rows)
        soft_groups.append({
            "group_id": f"G{index:02d}",
            "members": component,
            "mechanism_representatives": mechanism_representatives,
            "mechanism_archived_features": sorted(mechanism_archived),
        })

    singleton_analysis = []
    for component in components:
        if len(component) != 1:
            continue
        feature = component[0]
        peers = [other for other in feature_cols if other != feature]
        max_corr = max((abs(float(corr.loc[feature, other])) for other in peers if pd.notna(corr.loc[feature, other])), default=0.0)
        if max_corr < soft_threshold:
            reason = "singleton_because_truly_unique_under_soft_threshold"
        else:
            reason = "singleton_because_related_features_exist_but_hard_threshold_did_not_merge_them"
        singleton_analysis.append({
            "feature": feature,
            "mechanism_class": mechanism_class_from_label(feature, quality_rows.get(feature, {}).get("family")),
            "max_abs_spearman_corr": float(max_corr),
            "soft_threshold": soft_threshold,
            "reason": reason,
        })

    output_sets = _build_output_sets(clusters, soft_groups, quality_rows)
    payload = {
        "grouped_selection_policy": {
            "hard_duplicates": "single representative only",
            "soft_groups": "preserve one representative per mechanism class",
            "temporal_override": "temporal defaults to research_only unless it exceeds non-temporal candidates on both significance and baseline lift",
        },
        "threshold": threshold,
        "soft_threshold": soft_threshold,
        "feature_count": len(feature_cols),
        "high_corr_pairs": pairs,
        "soft_corr_pairs": soft_pairs,
        "clusters": clusters,
        "soft_groups": soft_groups,
        "singleton_analysis": singleton_analysis,
        "minimal_basis": minimal_basis,
        "output_sets": output_sets,
    }
    dump_json(output_path, payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--features")
    parser.add_argument("--threshold", type=float, default=0.85)
    parser.add_argument("--quality-input")
    parser.add_argument("--soft-threshold", type=float, default=0.50)
    args = parser.parse_args()
    run(
        Path(args.input),
        Path(args.output),
        features=args.features,
        threshold=float(args.threshold),
        quality_input=Path(args.quality_input) if args.quality_input else None,
        soft_threshold=float(args.soft_threshold),
    )


if __name__ == "__main__":
    main()
