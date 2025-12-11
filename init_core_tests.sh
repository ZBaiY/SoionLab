#!/usr/bin/env bash
set -e

ROOT_DIR="$(pwd)"
TEST_ROOT="$ROOT_DIR/tests"

echo "Project root:  $ROOT_DIR"
echo "Tests root:    $TEST_ROOT"
echo

# -----------------------------
# 1. 基础目录 + __init__.py
# -----------------------------
mkdir -p "$TEST_ROOT"
mkdir -p "$TEST_ROOT/unit"
mkdir -p "$TEST_ROOT/unit/contracts"
mkdir -p "$TEST_ROOT/unit/contracts/execution"
mkdir -p "$TEST_ROOT/unit/data/contracts"
mkdir -p "$TEST_ROOT/unit/data/ohlcv"

touch "$TEST_ROOT/__init__.py"
touch "$TEST_ROOT/unit/__init__.py"
touch "$TEST_ROOT/unit/contracts/__init__.py"
touch "$TEST_ROOT/unit/contracts/execution/__init__.py"
touch "$TEST_ROOT/unit/data/__init__.py"
touch "$TEST_ROOT/unit/data/contracts/__init__.py"
touch "$TEST_ROOT/unit/data/ohlcv/__init__.py"

create_if_missing () {
    local file_path="$1"
    if [ ! -f "$file_path" ]; then
        echo "Creating $file_path"
        cat > "$file_path" <<EOF_INNER
import pytest

# TODO: 根据 quant_engine 实现补充真实测试

def test_placeholder():
    assert True
EOF_INNER
    else
        echo "Skip existing $file_path"
    fi
}

# -----------------------------
# 2. contracts 层：核心协议 / TypedDict
#    对应 src/quant_engine/contracts/*
# -----------------------------
create_if_missing "$TEST_ROOT/unit/contracts/test_portfolio_contracts.py"
create_if_missing "$TEST_ROOT/unit/contracts/test_model_contracts.py"
create_if_missing "$TEST_ROOT/unit/contracts/test_feature_contracts.py"
create_if_missing "$TEST_ROOT/unit/contracts/test_decision_contracts.py"
create_if_missing "$TEST_ROOT/unit/contracts/test_risk_contracts.py"

# execution contracts: src/quant_engine/contracts/execution/*
create_if_missing "$TEST_ROOT/unit/contracts/execution/test_order_contracts.py"
create_if_missing "$TEST_ROOT/unit/contracts/execution/test_policy_contracts.py"
create_if_missing "$TEST_ROOT/unit/contracts/execution/test_router_contracts.py"
create_if_missing "$TEST_ROOT/unit/contracts/execution/test_slippage_contracts.py"
create_if_missing "$TEST_ROOT/unit/contracts/execution/test_matching_contracts.py"

# -----------------------------
# 3. data/contracts：最早的数据 handler 合同
#    对应 src/quant_engine/data/contracts/*
# -----------------------------
create_if_missing "$TEST_ROOT/unit/data/contracts/test_base_handler_contract.py"
create_if_missing "$TEST_ROOT/unit/data/contracts/test_ohlcv_handler_contract.py"
create_if_missing "$TEST_ROOT/unit/data/contracts/test_option_handler_contract.py"
create_if_missing "$TEST_ROOT/unit/data/contracts/test_orderbook_handler_contract.py"
create_if_missing "$TEST_ROOT/unit/data/contracts/test_sentiment_handler_contract.py"
create_if_missing "$TEST_ROOT/unit/data/contracts/test_iv_surface_handler_contract.py"

# -----------------------------
# 4. data/ohlcv：最开始走数据流程的文件
#    对应 src/quant_engine/data/ohlcv/*
# -----------------------------
create_if_missing "$TEST_ROOT/unit/data/ohlcv/test_loader_api.py"
create_if_missing "$TEST_ROOT/unit/data/ohlcv/test_historical_roundtrip.py"
create_if_missing "$TEST_ROOT/unit/data/ohlcv/test_snapshot_struct.py"
create_if_missing "$TEST_ROOT/unit/data/ohlcv/test_cache_behavior.py"

echo
echo "Core contracts + data(ohlcv) unit-test skeletons created."
