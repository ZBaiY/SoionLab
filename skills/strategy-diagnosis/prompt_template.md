Use $strategy-diagnosis on run <RUN_ID>.

Diagnose the strategy end-to-end from completed artifacts only.
Do not start with code changes or broad retuning.

Workflow:
1. identify the strategy/run set
2. confirm the replay source from artifacts
3. build regime labels
4. slice performance by major windows and regimes
5. diagnose behavior inside each regime
6. run post-decision drift
7. build the regime matrix
8. recommend at most 2 targeted next experiments

Answer:
- what the strategy is good at
- where it loses money
- whether the main weakness is entry, exit, regime mismatch, or sizing/
execution

Use outputs under:
- artifacts/analysis/<RUN_ID_LOWER>/

--------------------------------------------------------------
--------------------------------------------------------------

If you want the version that also supports sibling comparison, use this:

Use $strategy-diagnosis on run <PRIMARY_RUN_ID>.

Optional comparison runs:
- <RUN_ID_2>
- <RUN_ID_3>

Diagnose from completed artifacts only.

Workflow:
1. identify the strategy set
2. confirm replay sources
3. build regime labels
4. slice performance by windows and regimes
5. diagnose behavior inside each regime
6. run post-decision drift
7. compare sibling/counterfactual variants if provided
8. build the regime matrix
9. recommend at most 2 targeted next experiments

Answer:
- what each strategy is good at
- where each struggles
- what actually explains the differences

Write outputs under:
- artifacts/analysis/<ANALYSIS_SLUG>/