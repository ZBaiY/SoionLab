import pandas as pd

from ingestion.option_chain.normalize import DeribitOptionChainNormalizer


def test_normalize_list_payload_core_and_aux() -> None:
    raw = [
        {
            "instrument_name": "BTC-30NOV25-91000-C",
            "expiration_timestamp": 1764470400000,
            "strike": "91000",
            "option_type": "call",
            "foo": 1,
        },
        {
            "instrument_name": "BTC-30NOV25-92000-P",
            "expiration_timestamp": 1764470400000,
            "strike": 92000,
            "option_type": "put",
            "foo": 2,
        },
    ]

    norm = DeribitOptionChainNormalizer(symbol="BTC")
    payload = norm.normalize(raw)

    assert "data_ts" in payload
    assert "frame" in payload

    frame = payload["frame"]
    assert list(frame.columns) == ["instrument_name", "expiry_ts", "strike", "cp", "aux"]
    assert frame["cp"].tolist() == ["C", "P"]
    assert frame["expiry_ts"].dtype == "int64"
    assert frame["strike"].dtype == "float64"
    assert isinstance(frame.loc[0, "aux"], dict)
    assert frame.loc[0, "aux"]["foo"] == 1
    assert frame.loc[0, "aux"]["option_type"] == "call"
    assert frame.loc[0, "aux"]["expiration_timestamp"] == 1764470400000


def test_normalize_dataframe_with_data_ts_column() -> None:
    df = pd.DataFrame(
        [
            {
                "data_ts": 123,
                "instrument_name": "BTC-30NOV25-91000-C",
                "expiration_timestamp": 1764470400000,
                "strike": 91000,
            }
        ]
    )
    norm = DeribitOptionChainNormalizer(symbol="BTC")
    payload = norm.normalize(df)

    assert payload["data_ts"] == 123
    frame = payload["frame"]
    assert "data_ts" not in frame.columns
    assert frame.loc[0, "cp"] == "C"
