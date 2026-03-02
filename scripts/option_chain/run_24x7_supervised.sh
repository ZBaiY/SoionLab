#!/usr/bin/env bash
set -euo pipefail

# Month-long safe profile:
# - writer process enabled
# - periodic writer recycle
# - tree RSS guardrail restart
#
# Usage:
#   scripts/option_chain/run_24x7_supervised.sh [RSS_MAX_MB]
#
# Example:
#   scripts/option_chain/run_24x7_supervised.sh 2500

RSS_MAX_MB="${1:-2500}"

exec python3 scripts/option_chain/rss_guard_supervisor.py \
  --tree-rss \
  --rss-max-mb "${RSS_MAX_MB}" \
  --sample-interval-s 5 \
  --cooldown-s 15 \
  --graceful-stop-s 20 \
  -- \
  python3 apps/scrap/option_chain.py \
    --currencies BTC,ETH \
    --intervals 1m,5m \
    --writer-process true \
    --writer-queue-size 64 \
    --writer-put-timeout-s 5 \
    --writer-recycle-seconds 60 \
    --writer-recycle-tasks 300
