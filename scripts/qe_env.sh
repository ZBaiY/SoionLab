#!/usr/bin/env bash

# Shared SoionLab environment defaults for the `qe` conda env.
# Intended to be sourced:
#   source scripts/qe_env.sh
#   source scripts/qe_env.sh mainnet
#
# Behavior:
# - Sets safe defaults only when variables are unset.
# - Never overwrites existing credential values.
# - Keeps mainnet safety gates opt-in.

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "source this script instead of executing it: source scripts/qe_env.sh [testnet|mainnet]" >&2
  exit 1
fi

profile="${1:-${BINANCE_ENV:-testnet}}"
case "${profile}" in
  testnet|mainnet)
    ;;
  *)
    echo "invalid profile: ${profile}. expected testnet or mainnet" >&2
    return 1
    ;;
esac

export BINANCE_ENV="${BINANCE_ENV:-$profile}"

# Realtime cadence defaults.
export REALTIME_STEP_DELAY_MS="${REALTIME_STEP_DELAY_MS:-5000}"
export REALTIME_OHLCV_POLL_INTERVAL_MS="${REALTIME_OHLCV_POLL_INTERVAL_MS:-3000}"
export REALTIME_OPTION_CHAIN_POLL_INTERVAL_MS="${REALTIME_OPTION_CHAIN_POLL_INTERVAL_MS:-5000}"

# Safety gates stay explicit by default.
export BINANCE_MAINNET_CONFIRM="${BINANCE_MAINNET_CONFIRM:-}"
export BINANCE_BASE_URL_CONFIRM="${BINANCE_BASE_URL_CONFIRM:-}"
export BINANCE_PROXY_MODE="${BINANCE_PROXY_MODE:-}"
export LIVE_ALLOW_NONFLAT_START="${LIVE_ALLOW_NONFLAT_START:-}"

# Preserve explicit URL overrides if already provided.
export BINANCE_BASE_URL="${BINANCE_BASE_URL:-}"
export DERIBIT_BASE_URL="${DERIBIT_BASE_URL:-}"

if [[ "${BINANCE_ENV}" == "testnet" ]]; then
  export BINANCE_TESTNET_API_KEY="${BINANCE_TESTNET_API_KEY:-}"
  export BINANCE_TESTNET_API_SECRET="${BINANCE_TESTNET_API_SECRET:-}"
else
  export BINANCE_MAINNET_API_KEY="${BINANCE_MAINNET_API_KEY:-}"
  export BINANCE_MAINNET_API_SECRET="${BINANCE_MAINNET_API_SECRET:-}"
fi

