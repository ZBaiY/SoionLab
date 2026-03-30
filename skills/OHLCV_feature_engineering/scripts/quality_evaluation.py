from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Any, Callable

import pandas as pd

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import (
    QUALITY_DIMENSIONS,
    dump_json,
    feature_role_from_label,
    load_json,
    load_table,
    read_template,
    score_average,
    write_text,
)


ROLE_CONFIGS: dict[str, dict[str, Any]] = {
    "alpha_feature": {
        "primary_metrics": ["overall_rank_ic", "oos_rank_ic", "walk_forward_sign_consistency", "walk_forward_ic_dispersion"],
        "weights": {
            "overall_rank_ic": 0.25,
            "oos_rank_ic": 0.35,
            "walk_forward_sign_consistency": 0.20,
            "walk_forward_ic_dispersion": 0.20,
        },
        "thresholds": {"keep": 0.62, "review": 0.48},
    },
    "regime_feature": {
        "primary_metrics": ["conditional_abs_return_spread", "abs_return_monotonicity", "state_persistence"],
        "weights": {
            "conditional_abs_return_spread": 0.40,
            "abs_return_monotonicity": 0.30,
            "state_persistence": 0.30,
        },
        "thresholds": {"keep": 0.58, "review": 0.42},
    },
    "risk_feature": {
        "primary_metrics": ["abs_return_linkage", "tail_risk_spread", "state_persistence"],
        "weights": {
            "abs_return_linkage": 0.40,
            "tail_risk_spread": 0.35,
            "state_persistence": 0.25,
        },
        "thresholds": {"keep": 0.58, "review": 0.44},
    },
    "activity_feature": {
        "primary_metrics": ["activity_abs_return_spread", "activity_abs_return_linkage", "state_persistence"],
        "weights": {
            "activity_abs_return_spread": 0.35,
            "activity_abs_return_linkage": 0.25,
            "state_persistence": 0.40,
        },
        "thresholds": {"keep": 0.56, "review": 0.42},
    },
    "temporal_feature": {
        "primary_metrics": ["periodic_target_alignment", "periodic_abs_target_alignment", "monthly_phase_consistency"],
        "weights": {
            "periodic_target_alignment": 0.30,
            "periodic_abs_target_alignment": 0.35,
            "monthly_phase_consistency": 0.35,
        },
        "thresholds": {"keep": 0.55, "review": 0.40},
    },
}

METRIC_SPECS: dict[str, dict[str, str]] = {
    "overall_rank_ic": {"direction": "magnitude_higher_better"},
    "oos_rank_ic": {"direction": "magnitude_higher_better"},
    "walk_forward_sign_consistency": {"direction": "positive_higher_better"},
    "walk_forward_ic_dispersion": {"direction": "lower_better"},
    "conditional_abs_return_spread": {"direction": "magnitude_higher_better"},
    "abs_return_monotonicity": {"direction": "magnitude_higher_better"},
    "state_persistence": {"direction": "magnitude_higher_better"},
    "abs_return_linkage": {"direction": "magnitude_higher_better"},
    "tail_risk_spread": {"direction": "magnitude_higher_better"},
    "activity_abs_return_spread": {"direction": "magnitude_higher_better"},
    "activity_abs_return_linkage": {"direction": "magnitude_higher_better"},
    "periodic_target_alignment": {"direction": "magnitude_higher_better"},
    "periodic_abs_target_alignment": {"direction": "magnitude_higher_better"},
    "monthly_phase_consistency": {"direction": "magnitude_higher_better"},
}

ROLE_SIMPLE_BASELINES = {
    "alpha_feature": "log_return_1",
    "regime_feature": "trend_slope_8",
    "risk_feature": "garman_klass_8",
    "activity_feature": "rel_volume_20",
    "temporal_feature": "hour_sin",
}
PERMUTATION_COUNT = 24


def score_feature(spec: dict[str, Any]) -> dict[str, int]:
    complexity = 5
    param_count = sum(1 for value in spec["formula"]["parameters"].values() if value not in (None, {}, []))
    if param_count >= 3:
        complexity = 3
    if spec["family"] == "cross_interaction":
        complexity = min(complexity, 3)
    if spec["family"] == "trade_activity":
        complexity = min(complexity, 3)
    if spec["audit_status"] == "reject":
        return {key: 1 for key in QUALITY_DIMENSIONS}

    scores = {
        "causal_safety": 5 if spec["audit_status"] == "pass" else 3,
        "decision_time_clarity": 5 if spec["timing_semantics"]["bar_state"] == "closed_only" else 2,
        "interpretability": 5 if spec["family"] in {"price_path", "volatility", "volume_liquidity"} else 4,
        "implementation_complexity": complexity,
        "expected_incremental_information": 4 if spec["family"] in {"cross_interaction", "order_flow_proxy", "trade_activity"} else 3,
        "data_efficiency": 5 if spec["warmup"]["minimum_history_bars"] <= 20 else 3,
        "robustness_to_regime_shift": 4 if spec["family"] in {"volatility", "volume_liquidity", "time_structure"} else 3,
        "overfit_risk": 4 if spec["family"] in {"price_path", "volatility", "volume_liquidity"} else 3,
    }
    return scores


def run_definition_quality(input_path: Path, json_output: Path, md_output: Path) -> dict[str, Any]:
    payload = load_json(input_path)
    family_best: dict[str, tuple[str, float]] = {}
    rows: list[dict[str, Any]] = []

    for spec in payload["feature_specifications"]:
        scores = score_feature(spec)
        avg_score = score_average(scores)
        redundancy_group = f"{spec['family']}::{payload['parsed_directions']['signal_mode']}"
        verdict = "keep"
        if spec["audit_status"] == "reject":
            verdict = "reject"
        elif spec["audit_status"] == "clarify":
            verdict = "defer"

        previous = family_best.get(redundancy_group)
        dropped_in_favor_of = None
        if previous is None or avg_score > previous[1]:
            if previous is not None:
                for row in rows:
                    if row["feature_id"] == previous[0] and row["verdict"] == "keep":
                        row["verdict"] = "defer"
                        row["dropped_in_favor_of"] = spec["feature_id"]
                        row["weaknesses"].append("Superseded by a stronger feature in the same redundancy group.")
            family_best[redundancy_group] = (spec["feature_id"], avg_score)
        elif verdict == "keep":
            verdict = "defer"
            dropped_in_favor_of = previous[0]

        rows.append({
            "feature_id": spec["feature_id"],
            "feature_role": spec.get("feature_role") or feature_role_from_label(spec["feature_type_label"], spec["family"]),
            "verdict": verdict,
            "redundancy_group": redundancy_group,
            "dropped_in_favor_of": dropped_in_favor_of,
            "scores": scores,
            "strengths": [spec["description"], spec["audit_notes"][0] if spec["audit_notes"] else "No audit note."],
            "weaknesses": ([] if spec["audit_status"] == "pass" else spec["audit_notes"][:]),
            "failure_modes": next(
                item["failure_modes"] for item in payload["family_classification"] if item["feature_id"] == spec["feature_id"]
            ),
        })

    result = {"parsed_directions": payload["parsed_directions"], "quality_report": rows}
    dump_json(json_output, result)
    kept = [row for row in rows if row["verdict"] == "keep"]
    deferred = [row for row in rows if row["verdict"] == "defer"]
    rejected = [row for row in rows if row["verdict"] == "reject"]
    feature_lines = [
        f"- `{row['feature_id']}`: `{row['verdict']}`; role `{row['feature_role']}`; scores={row['scores']}"
        for row in rows
    ]
    template = read_template("quality_report_template.md")
    write_text(
        md_output,
        template.format(
            direction_id=payload["parsed_directions"]["direction_id"],
            observation_interval=payload["parsed_directions"]["observation_interval"],
            keep_count=len(kept),
            defer_count=len(deferred),
            reject_count=len(rejected),
            feature_rows="\n".join(feature_lines),
        ),
    )
    return result


def _pair_frame(feature: pd.Series, target: pd.Series) -> pd.DataFrame:
    frame = pd.concat([feature.rename("feature"), target.rename("target")], axis=1).dropna()
    return frame


def _safe_corr(feature: pd.Series, target: pd.Series, *, method: str = "spearman") -> tuple[float, int]:
    frame = _pair_frame(feature, target)
    if len(frame) < 5 or frame["feature"].nunique() < 2 or frame["target"].nunique() < 2:
        return 0.0, int(len(frame))
    value = frame["feature"].corr(frame["target"], method=method)
    return (0.0 if pd.isna(value) else float(value), int(len(frame)))


def _quantile_group_means(feature: pd.Series, response: pd.Series, *, q: int = 5) -> tuple[pd.Series, int]:
    frame = pd.concat([feature.rename("feature"), response.rename("response")], axis=1).dropna()
    if len(frame) < max(20, q * 4):
        return pd.Series(dtype=float), int(len(frame))
    try:
        buckets = pd.qcut(frame["feature"], q=q, labels=False, duplicates="drop")
    except ValueError:
        return pd.Series(dtype=float), int(len(frame))
    grouped = frame.groupby(buckets)["response"].mean().sort_index()
    return grouped, int(len(frame))


def _split_test_ics(feature: pd.Series, target: pd.Series, timestamp: pd.Series, splits: list[dict[str, Any]]) -> list[float]:
    frame = pd.concat(
        [timestamp.rename("timestamp"), feature.rename("feature"), target.rename("target")],
        axis=1,
    ).dropna()
    results: list[float] = []
    for split in splits:
        test = split["test"]
        sample = frame[(frame["timestamp"] >= test["start_ts"]) & (frame["timestamp"] <= test["end_ts"])]
        if len(sample) < 5 or sample["feature"].nunique() < 2 or sample["target"].nunique() < 2:
            continue
        value = sample["feature"].corr(sample["target"], method="spearman")
        if pd.notna(value):
            results.append(float(value))
    return results


def _monthly_corr_mean(feature: pd.Series, target: pd.Series, timestamp: pd.Series) -> tuple[float, int]:
    frame = pd.concat(
        [timestamp.rename("timestamp"), feature.rename("feature"), target.rename("target")],
        axis=1,
    ).dropna()
    if len(frame) < 48:
        return 0.0, int(len(frame))
    month = pd.to_datetime(frame["timestamp"], unit="ms", utc=True).dt.to_period("M")
    values: list[float] = []
    for _, sample in frame.groupby(month):
        if len(sample) < 20 or sample["feature"].nunique() < 2 or sample["target"].nunique() < 2:
            continue
        value = sample["feature"].corr(sample["target"], method="spearman")
        if pd.notna(value):
            values.append(abs(float(value)))
    return (float(sum(values) / len(values)) if values else 0.0, len(values))


def _state_persistence(feature: pd.Series) -> tuple[float, int]:
    frame = feature.dropna()
    if len(frame) < 5 or frame.nunique() < 2:
        return 0.0, int(len(frame))
    shifted = frame.shift(1)
    joined = pd.concat([frame.rename("feature"), shifted.rename("lag")], axis=1).dropna()
    if len(joined) < 5 or joined["lag"].nunique() < 2:
        return 0.0, int(len(joined))
    value = joined["feature"].corr(joined["lag"], method="spearman")
    return (0.0 if pd.isna(value) else abs(float(value)), int(len(joined)))


def _metric_overall_rank_ic(frame: pd.DataFrame, feature_name: str, _: list[dict[str, Any]]) -> tuple[float, int]:
    return _safe_corr(frame[feature_name], frame["target_forward_return_1"], method="spearman")


def _metric_oos_rank_ic(frame: pd.DataFrame, feature_name: str, splits: list[dict[str, Any]]) -> tuple[float, int]:
    values = _split_test_ics(frame[feature_name], frame["target_forward_return_1"], frame["timestamp"], splits)
    return (float(sum(values) / len(values)) if values else 0.0, len(values))


def _metric_walk_forward_sign_consistency(frame: pd.DataFrame, feature_name: str, splits: list[dict[str, Any]]) -> tuple[float, int]:
    overall, _ = _metric_overall_rank_ic(frame, feature_name, splits)
    values = _split_test_ics(frame[feature_name], frame["target_forward_return_1"], frame["timestamp"], splits)
    if not values or overall == 0.0:
        return 0.0, len(values)
    ref_sign = math.copysign(1.0, overall)
    hits = [1.0 if value != 0.0 and math.copysign(1.0, value) == ref_sign else 0.0 for value in values]
    return float(sum(hits) / len(hits)), len(hits)


def _metric_walk_forward_ic_dispersion(frame: pd.DataFrame, feature_name: str, splits: list[dict[str, Any]]) -> tuple[float, int]:
    values = _split_test_ics(frame[feature_name], frame["target_forward_return_1"], frame["timestamp"], splits)
    if len(values) < 2:
        return 0.0, len(values)
    return float(pd.Series(values, dtype=float).std(ddof=0)), len(values)


def _metric_conditional_abs_return_spread(frame: pd.DataFrame, feature_name: str, _: list[dict[str, Any]]) -> tuple[float, int]:
    means, n = _quantile_group_means(frame[feature_name], frame["target_forward_return_1"].abs())
    if len(means) < 2:
        return 0.0, n
    return float(means.iloc[-1] - means.iloc[0]), n


def _metric_abs_return_monotonicity(frame: pd.DataFrame, feature_name: str, _: list[dict[str, Any]]) -> tuple[float, int]:
    means, n = _quantile_group_means(frame[feature_name], frame["target_forward_return_1"].abs())
    if len(means) < 3:
        return 0.0, n
    order = pd.Series(range(len(means)), dtype=float)
    value = order.corr(means.reset_index(drop=True), method="spearman")
    return (0.0 if pd.isna(value) else abs(float(value)), n)


def _metric_state_persistence(frame: pd.DataFrame, feature_name: str, _: list[dict[str, Any]]) -> tuple[float, int]:
    return _state_persistence(frame[feature_name])


def _metric_abs_return_linkage(frame: pd.DataFrame, feature_name: str, _: list[dict[str, Any]]) -> tuple[float, int]:
    value, n = _safe_corr(frame[feature_name], frame["target_forward_return_1"].abs(), method="spearman")
    return abs(value), n


def _metric_tail_risk_spread(frame: pd.DataFrame, feature_name: str, _: list[dict[str, Any]]) -> tuple[float, int]:
    means, n = _quantile_group_means(frame[feature_name], frame["target_forward_return_1"].abs(), q=10)
    if len(means) < 2:
        return 0.0, n
    return float(means.iloc[-1] - means.iloc[0]), n


def _metric_activity_abs_return_spread(frame: pd.DataFrame, feature_name: str, _: list[dict[str, Any]]) -> tuple[float, int]:
    return _metric_conditional_abs_return_spread(frame, feature_name, [])


def _metric_activity_abs_return_linkage(frame: pd.DataFrame, feature_name: str, _: list[dict[str, Any]]) -> tuple[float, int]:
    return _metric_abs_return_linkage(frame, feature_name, [])


def _metric_periodic_target_alignment(frame: pd.DataFrame, feature_name: str, _: list[dict[str, Any]]) -> tuple[float, int]:
    value, n = _safe_corr(frame[feature_name], frame["target_forward_return_1"], method="spearman")
    return abs(value), n


def _metric_periodic_abs_target_alignment(frame: pd.DataFrame, feature_name: str, _: list[dict[str, Any]]) -> tuple[float, int]:
    value, n = _safe_corr(frame[feature_name], frame["target_forward_return_1"].abs(), method="spearman")
    return abs(value), n


def _metric_monthly_phase_consistency(frame: pd.DataFrame, feature_name: str, _: list[dict[str, Any]]) -> tuple[float, int]:
    return _monthly_corr_mean(frame[feature_name], frame["target_forward_return_1"], frame["timestamp"])


METRIC_FUNCTIONS: dict[str, Callable[[pd.DataFrame, str, list[dict[str, Any]]], tuple[float, int]]] = {
    "overall_rank_ic": _metric_overall_rank_ic,
    "oos_rank_ic": _metric_oos_rank_ic,
    "walk_forward_sign_consistency": _metric_walk_forward_sign_consistency,
    "walk_forward_ic_dispersion": _metric_walk_forward_ic_dispersion,
    "conditional_abs_return_spread": _metric_conditional_abs_return_spread,
    "abs_return_monotonicity": _metric_abs_return_monotonicity,
    "state_persistence": _metric_state_persistence,
    "abs_return_linkage": _metric_abs_return_linkage,
    "tail_risk_spread": _metric_tail_risk_spread,
    "activity_abs_return_spread": _metric_activity_abs_return_spread,
    "activity_abs_return_linkage": _metric_activity_abs_return_linkage,
    "periodic_target_alignment": _metric_periodic_target_alignment,
    "periodic_abs_target_alignment": _metric_periodic_abs_target_alignment,
    "monthly_phase_consistency": _metric_monthly_phase_consistency,
}


def _z_to_p_value(z_stat: float) -> float:
    return float(math.erfc(abs(z_stat) / math.sqrt(2.0)))


def _oriented_value(value: float, direction: str) -> float:
    if direction == "magnitude_higher_better":
        return abs(value)
    if direction == "positive_higher_better":
        return max(value, 0.0)
    if direction == "lower_better":
        return -value
    return value


def _global_aligned_frame(evaluation_root: Path) -> pd.DataFrame:
    matrix_path = evaluation_root / "all_features__btcusdt_15m_2025" / "feature_matrix.csv"
    target_path = evaluation_root / "price_path__btcusdt_15m_2025" / "target_matrix.csv"
    matrix = load_table(matrix_path)
    target = load_table(target_path)[["timestamp", "symbol", "target_forward_return_1"]]
    merged = matrix.merge(target, on=["timestamp", "symbol"], how="inner")
    return merged.sort_values("timestamp").reset_index(drop=True)


def _family_feature_map(evaluation_root: Path) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for directory in sorted(path for path in evaluation_root.iterdir() if path.is_dir() and "__" in path.name):
        if directory.name.startswith("all_features__"):
            continue
        family = directory.name.split("__", 1)[0]
        payload = load_json(directory / "ic_analysis.json")
        for feature in payload.get("features", []):
            mapping[feature] = family
    return mapping


def _load_splits(evaluation_root: Path) -> list[dict[str, Any]]:
    payload = load_json(evaluation_root / "price_path__btcusdt_15m_2025" / "splits.json")
    return payload.get("splits", [])


def _permuted_feature(series: pd.Series, seed: int) -> pd.Series:
    values = series.dropna().sample(frac=1.0, random_state=seed).to_numpy()
    result = pd.Series(index=series.dropna().index, data=values)
    return result.reindex(series.index)


def _metric_with_evidence(frame: pd.DataFrame, feature_name: str, metric_name: str,
                          baseline_feature: str, splits: list[dict[str, Any]]) -> dict[str, Any]:
    metric_fn = METRIC_FUNCTIONS[metric_name]
    raw, sample_size = metric_fn(frame, feature_name, splits)

    random_values = []
    for seed in range(PERMUTATION_COUNT):
        permuted = frame.copy()
        permuted[feature_name] = _permuted_feature(frame[feature_name], seed)
        value, _ = metric_fn(permuted, feature_name, splits)
        random_values.append(float(value))
    random_series = pd.Series(random_values, dtype=float)
    baseline_random = float(random_series.mean()) if len(random_series) else 0.0
    random_std = float(random_series.std(ddof=0)) if len(random_series) > 1 else 0.0

    simple_feature = baseline_feature if baseline_feature in frame.columns else feature_name
    baseline_simple, _ = metric_fn(frame, simple_feature, splits)

    direction = METRIC_SPECS[metric_name]["direction"]
    oriented_raw = _oriented_value(float(raw), direction)
    oriented_random = _oriented_value(float(baseline_random), direction)
    oriented_simple = _oriented_value(float(baseline_simple), direction)
    z_stat = (oriented_raw - oriented_random) / max(random_std, 1e-9)
    p_value = _z_to_p_value(z_stat)
    return {
        "raw": float(raw),
        "sample_size": int(sample_size),
        "t_stat": float(z_stat),
        "p_value": float(p_value),
        "baseline_random": float(baseline_random),
        "baseline_simple": float(baseline_simple),
        "lift_vs_random": float(oriented_raw - oriented_random),
        "lift_vs_simple": float(oriented_raw - oriented_simple),
        "normalized_score": 0.0,
        "scale_reference": {
            "method": "empirical_random_null_std",
            "value": max(random_std, 1e-9),
            "peer_count": len(random_values),
        },
    }


def _apply_empirical_metric_ranks(rows: list[dict[str, Any]]) -> None:
    metric_tstats: dict[str, list[tuple[str, float]]] = {}
    for row in rows:
        for metric_name in row["primary_metrics"]:
            metric_tstats.setdefault(metric_name, []).append((row["feature"], float(row["metrics"][metric_name]["t_stat"])))

    for metric_name, pairs in metric_tstats.items():
        series = pd.Series({feature: value for feature, value in pairs}, dtype=float)
        ranks = series.rank(method="average", pct=True)
        for row in rows:
            if metric_name not in row["metrics"]:
                continue
            row["metrics"][metric_name]["normalized_score"] = float(ranks[row["feature"]])
            row["metrics"][metric_name]["scale_reference"]["ranking_method"] = "empirical_metric_percentile"
            row["metrics"][metric_name]["scale_reference"]["metric_peer_count"] = int(len(series))


def run_empirical_quality(evaluation_root: Path, json_output: Path, md_output: Path) -> dict[str, Any]:
    aligned = _global_aligned_frame(evaluation_root)
    feature_families = _family_feature_map(evaluation_root)
    splits = _load_splits(evaluation_root)

    rows: list[dict[str, Any]] = []
    for feature_name, family in sorted(feature_families.items()):
        role = feature_role_from_label(feature_name, family)
        baseline_feature = ROLE_SIMPLE_BASELINES[role]
        metrics = {
            metric_name: _metric_with_evidence(aligned, feature_name, metric_name, baseline_feature, splits)
            for metric_name in ROLE_CONFIGS[role]["primary_metrics"]
        }
        weights = ROLE_CONFIGS[role]["weights"]
        decision_score = sum(metrics[name]["normalized_score"] * weights[name] for name in weights) / sum(weights.values())
        thresholds = ROLE_CONFIGS[role]["thresholds"]
        if decision_score >= thresholds["keep"]:
            decision = "keep"
        elif decision_score >= thresholds["review"]:
            decision = "review"
        else:
            decision = "reject"
        rows.append({
            "family": family,
            "feature": feature_name,
            "feature_role": role,
            "primary_metrics": ROLE_CONFIGS[role]["primary_metrics"],
            "metrics": metrics,
            "decision": decision,
            "decision_score": {
                "raw": float(decision_score),
                "normalized_score": float(decision_score),
                "sample_size": len(weights),
                "t_stat": None,
                "p_value": None,
                "baseline_random": None,
                "baseline_simple": None,
                "lift_vs_random": None,
                "lift_vs_simple": None,
                "scale_reference": {"method": "weighted_metric_average", "value": 1.0, "peer_count": len(weights)},
            },
        })

    _apply_empirical_metric_ranks(rows)
    for row in rows:
        weights = ROLE_CONFIGS[row["feature_role"]]["weights"]
        decision_score = sum(row["metrics"][name]["normalized_score"] * weights[name] for name in weights) / sum(weights.values())
        thresholds = ROLE_CONFIGS[row["feature_role"]]["thresholds"]
        if decision_score >= thresholds["keep"]:
            decision = "keep"
        elif decision_score >= thresholds["review"]:
            decision = "review"
        else:
            decision = "reject"
        row["decision"] = decision
        row["decision_score"] = {
            "raw": float(decision_score),
            "normalized_score": float(decision_score),
            "sample_size": len(weights),
            "t_stat": None,
            "p_value": None,
            "baseline_random": None,
            "baseline_simple": None,
            "lift_vs_random": None,
            "lift_vs_simple": None,
            "scale_reference": {"method": "weighted_metric_percentile_average", "value": 1.0, "peer_count": len(weights)},
        }

    result = {
        "metric_schema": {
            "normalization_policy": "empirical percentile rank of metric t_stat across evaluated feature set; no fixed score bands",
            "normalized_score_formula": "rank_pct(t_stat | metric_name, evaluated_feature_set)",
            "metrics": METRIC_SPECS,
            "roles": ROLE_CONFIGS,
        },
        "feature_evaluations": rows,
    }
    dump_json(json_output, result)

    lines = []
    for row in rows:
        metric_bits = []
        for metric_name in row["primary_metrics"]:
            metric = row["metrics"][metric_name]
            metric_bits.append(
                f"{metric_name}=raw:{metric['raw']:.6f},score:{metric['normalized_score']:.3f},"
                f"t:{metric['t_stat']:.3f},p:{metric['p_value']:.4f}"
            )
        lines.append(
            f"- `{row['feature']}` [{row['feature_role']}] => `{row['decision']}` "
            f"(decision_score={row['decision_score']['raw']:.3f}); " + "; ".join(metric_bits)
        )
    write_text(md_output, "# Empirical Quality Report\n\n" + "\n".join(lines) + "\n")
    return result


def run(input_path: Path, json_output: Path, md_output: Path) -> dict[str, Any]:
    return run_definition_quality(input_path, json_output, md_output)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--evaluation-root")
    parser.add_argument("--json-output", required=True)
    parser.add_argument("--md-output", required=True)
    args = parser.parse_args()
    if args.evaluation_root:
        run_empirical_quality(Path(args.evaluation_root), Path(args.json_output), Path(args.md_output))
        return
    if not args.input:
        raise SystemExit("--input is required when --evaluation-root is not provided")
    run_definition_quality(Path(args.input), Path(args.json_output), Path(args.md_output))


if __name__ == "__main__":
    main()
