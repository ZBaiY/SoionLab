# OHLCV Feature Engineering Roadmap

Internal handbook for producing OHLCV feature research artifacts in the SoionLab repo.

## 1. Scope

This skill does not implement features.
It produces:
- parsed directions
- candidate feature specs
- timing and leakage audit results
- family classification
- quality screening
- implementation handoffs
- library records
- feature-research evaluation artifacts

## 2. Placement

Reusable skill logic belongs under:

```text
skills/OHLCV_feature_engineering/
  SKILL.md
  roadmap.md
  runner.md
  TIMING_RULES.md
  scripts/
  schemas/
```

Durable outputs belong under:

```text
research_library/ohlcv_feature_engineering/
  registry/
  specs/
  reports/
  guidelines/
  runs/
    YYYYMMDD/
      <record_id>/
        parsed_directions.json
        classified_features.json
        feature_specs.yaml
        quality_report.md
        implementation_handoff.md
        writer_guidelines.md
```

Layout rule:
- `specs/`, `reports/`, and `guidelines/` are canonical, deduplicated artifact stores.
- `runs/YYYYMMDD/<record_id>/` is the per-run bundle for inspection, replay, and future comparison.
- Every skill execution should write a run bundle even when the canonical spec/report/guideline resolves to an existing deduplicated artifact.

Feature code that another agent may implement later belongs under:
- `src/quant_engine/features/`

Strategy wiring that another agent may add later belongs under:
- `apps/strategy/`

## 3. Runtime Grounding

Feature research must match repo timing semantics:
- Driver owns `step_ts`
- observation interval is semantic time
- OHLCV `data_ts` is event time
- visible bars must satisfy `data_ts <= visible_end_ts(step_ts)`
- ingestion poll cadence is not feature time

## 4. Canonical Families

Use only:
- `price_path`
- `volatility`
- `volume_liquidity`
- `order_flow_proxy`
- `trade_activity`
- `time_structure`
- `cross_interaction`

## 5. Standard Outputs

Each run should end with:
- `parsed_directions.json`
- `feature_specs.yaml`
- `quality_report.md`
- `implementation_handoff.md`
- `writer_guidelines.md`
- `library_record.json`
- a copied per-run bundle under `research_library/ohlcv_feature_engineering/runs/YYYYMMDD/<record_id>/`

Evaluation runs should end with:
- `target_matrix.csv`
- `aligned_feature_matrix.csv`
- `ic_analysis.json`
- `stability_diagnostics.json`
- `splits.json`
- `walk_forward_validation.json`
- `redundancy_analysis.json`
- `evaluation_quality_report.md`

## 6. Acceptance Standard

A research run is valid only if:
- feature count is bounded
- timing audit was executed
- at least one candidate is either `keep`, `defer`, or explicit `reject`
- implementation handoff is specific enough for a downstream coding agent
- library writeback did not silently overwrite an existing record
