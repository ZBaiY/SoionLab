# Regime Definitions

Use these as the default coarse regime labels for this repo.
This is a diagnosis taxonomy, not a fixed indicator recipe.

## Bar-Level Inputs

Preferred inputs:
- close price from trace market snapshots or another reliable market series
- one trend-strength proxy if available
- one direction proxy if available
- recent realized volatility
- bar return over a short lookback

Possible trend-strength proxies:
- ADX
- rolling directional efficiency
- breakout distance from trend baseline
- moving-average slope magnitude

Possible direction proxies:
- DMI spread
- moving-average slope sign
- momentum sign
- breakout direction

## Default Labels

### `trend_up`
- strong trend proxy
- bullish direction proxy
- short-lookback return positive

### `trend_down`
- strong trend proxy
- bearish direction proxy
- short-lookback return negative

### `sideways_low_vol`
- weak trend proxy
- realized volatility below chosen threshold

### `choppy_high_vol`
- weak or unstable trend proxy
- realized volatility elevated
- direction not persistent

### `breakout_post_breakout`
- large positive short-horizon return
- volatility elevated
- direction consistent with DMI

### `liquidation_panic_reversal`
- large negative short-horizon return
- volatility elevated
- drawdown acceleration or strong negative drift

## Notes

- These labels are coarse diagnostics, not a production trading model.
- Keep thresholds stable during one comparison. Do not redefine regimes halfway through analysis.
- If the strategy itself uses one of the classifier inputs, say so explicitly: the regime classifier is partly aligned with the strategy’s own worldview.
- The right question is not “does this use ADX,” but “what is the best stable proxy for trend strength and direction in this strategy family.”
