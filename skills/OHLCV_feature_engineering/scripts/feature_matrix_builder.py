from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import load_table, parse_features_arg, write_table


EPS = 1e-12


def _recursive_anchor(typical_price: pd.Series, refresh: pd.Series) -> pd.Series:
    anchor = np.full(len(typical_price), np.nan, dtype=float)
    seeded = False
    for idx, (price, weight) in enumerate(zip(typical_price.to_numpy(dtype=float), refresh.to_numpy(dtype=float))):
        if np.isnan(price) or np.isnan(weight):
            continue
        if not seeded:
            anchor[idx] = price
            seeded = True
            continue
        w = float(np.clip(weight, 0.0, 1.0)) if not np.isnan(weight) else 0.0
        anchor[idx] = (1.0 - w) * anchor[idx - 1] + w * price
    return pd.Series(anchor, index=typical_price.index, dtype=float)


def _prepare(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    out = df.copy()
    if "data_ts" in out.columns:
        out["timestamp"] = out["data_ts"].astype("int64")
    elif "close_time" in out.columns:
        out["timestamp"] = out["close_time"].astype("int64")
    else:
        raise ValueError("Raw OHLCV input must contain data_ts or close_time.")
    out["symbol"] = symbol
    out = out.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    out["log_close"] = np.log(out["close"].clip(lower=EPS))
    out["log_return_1"] = out["log_close"].diff()
    out["range_proxy"] = (out["high"] - out["low"]) / out["open"].clip(lower=EPS)
    out["parkinson_var"] = (np.log(out["high"].clip(lower=EPS) / out["low"].clip(lower=EPS)) ** 2) / (4.0 * np.log(2.0))
    out["garman_klass_var"] = (
        0.5 * (np.log(out["high"].clip(lower=EPS) / out["low"].clip(lower=EPS)) ** 2)
        - (2.0 * np.log(2.0) - 1.0) * (np.log(out["close"].clip(lower=EPS) / out["open"].clip(lower=EPS)) ** 2)
    )
    out["true_range"] = out["high"] - out["low"]
    out["range_expansion_20"] = out["true_range"] / out["true_range"].rolling(20).mean().clip(lower=EPS)
    out["intrabar_asymmetry"] = (out["close"] - out["open"]) / (out["high"] - out["low"]).clip(lower=EPS)
    out["squared_return"] = out["log_return_1"] ** 2
    out["vwap_proxy"] = out["quote_asset_volume"] / out["volume"].clip(lower=EPS)
    out["vwap_proxy_premium"] = out["vwap_proxy"] / out["close"].clip(lower=EPS) - 1.0
    out["rel_volume_20"] = out["volume"] / out["volume"].rolling(20).mean().clip(lower=EPS)
    out["volume_zscore_20"] = (out["volume"] - out["volume"].rolling(20).mean()) / out["volume"].rolling(20).std().clip(lower=EPS)
    out["imbalance_base"] = (2.0 * out["taker_buy_base_asset_volume"] - out["volume"]) / out["volume"].clip(lower=EPS)
    out["buy_share_quote"] = out["taker_buy_quote_asset_volume"] / out["quote_asset_volume"].clip(lower=EPS)
    out["avg_trade_size"] = out["volume"] / out["number_of_trades"].clip(lower=EPS)
    out["trade_count_rel_20"] = out["number_of_trades"] / out["number_of_trades"].rolling(20).mean().clip(lower=EPS)
    out["trade_intensity"] = out["number_of_trades"] / 900.0
    out["hour_sin"] = np.sin(2.0 * np.pi * ((out["open_time"] // 3_600_000) % 24) / 24.0)
    out["hour_cos"] = np.cos(2.0 * np.pi * ((out["open_time"] // 3_600_000) % 24) / 24.0)
    out["day_of_week"] = ((out["open_time"] // 86_400_000) + 4) % 7
    out["typical_price"] = (out["high"] + out["low"] + out["close"]) / 3.0
    out["inventory_refresh_raw_32"] = out["quote_asset_volume"] / out["quote_asset_volume"].rolling(32).sum().clip(lower=EPS)
    out["inventory_refresh_32"] = out["inventory_refresh_raw_32"].clip(lower=0.0, upper=0.20)
    out["inventory_anchor_32"] = _recursive_anchor(out["typical_price"], out["inventory_refresh_32"])
    out["inventory_overhang"] = np.log(out["close"].clip(lower=EPS) / out["inventory_anchor_32"].clip(lower=EPS))
    out["inventory_pressure_z"] = (
        out["inventory_overhang"] - out["inventory_overhang"].rolling(64).mean()
    ) / out["inventory_overhang"].rolling(64).std().clip(lower=EPS)

    out["trend_slope_8"] = out["log_close"].rolling(8).apply(
        lambda x: float(np.polyfit(np.arange(len(x)), x, 1)[0]) if len(x) >= 2 else np.nan,
        raw=False,
    )
    out["path_efficiency_8"] = (out["close"] - out["close"].shift(8)).abs() / (
        out["close"].diff().abs().rolling(8).sum().clip(lower=EPS)
    )
    out["garman_klass_8"] = np.sqrt(out["garman_klass_var"].rolling(8).mean().clip(lower=0.0))
    out["parkinson_8"] = np.sqrt(out["parkinson_var"].rolling(8).mean().clip(lower=0.0))
    out["vol_ratio_5_20"] = out["log_return_1"].rolling(5).std() / out["log_return_1"].rolling(20).std().clip(lower=EPS)
    out["imbalance_x_return"] = out["imbalance_base"] * out["log_return_1"]
    out["imbalance_x_vol"] = out["imbalance_base"] * out["garman_klass_8"]
    out["inventory_overhang_x_imbalance"] = out["inventory_overhang"] * out["imbalance_base"]
    out["inventory_overhang_x_log_return"] = out["inventory_overhang"] * out["log_return_1"]
    out["inventory_overhang_x_range_expansion"] = out["inventory_overhang"] * out["range_expansion_20"]
    return out


FAMILY_FEATURES = {
    "price_path": ["log_return_1", "trend_slope_8"],
    "volatility": ["garman_klass_8", "intrabar_asymmetry"],
    "volume_liquidity": ["rel_volume_20", "vwap_proxy_premium"],
    "order_flow_proxy": ["imbalance_base", "imbalance_x_return"],
    "inventory_state": [
        "inventory_overhang",
        "inventory_pressure_z",
        "inventory_overhang_x_imbalance",
        "inventory_overhang_x_log_return",
        "inventory_overhang_x_range_expansion",
    ],
    "trade_activity": ["avg_trade_size", "trade_count_rel_20"],
    "time_structure": ["hour_sin", "hour_cos"],
    "all": [
        "log_return_1",
        "trend_slope_8",
        "garman_klass_8",
        "intrabar_asymmetry",
        "rel_volume_20",
        "vwap_proxy_premium",
        "imbalance_base",
        "imbalance_x_return",
        "inventory_overhang",
        "inventory_pressure_z",
        "inventory_overhang_x_imbalance",
        "inventory_overhang_x_log_return",
        "inventory_overhang_x_range_expansion",
        "avg_trade_size",
        "trade_count_rel_20",
        "hour_sin",
        "hour_cos",
    ],
}


def run(input_path: Path, output_path: Path, *, family: str, symbol: str, features: str | None = None) -> pd.DataFrame:
    df = _prepare(load_table(input_path), symbol)
    feature_cols = parse_features_arg(features, None) if features else FAMILY_FEATURES[family]
    columns = ["timestamp", "symbol", "close"] + feature_cols
    write_table(df[columns], output_path)
    return df[columns]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--family", required=True, choices=sorted(FAMILY_FEATURES.keys()))
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--features")
    args = parser.parse_args()
    run(Path(args.input), Path(args.output), family=args.family, symbol=args.symbol, features=args.features)


if __name__ == "__main__":
    main()
