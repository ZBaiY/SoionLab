from __future__ import annotations

import asyncio
import logging

import pytest

from quant_engine.utils.asyncio import iter_source


class _SyncSource:
    def __iter__(self):
        yield {"a": 1}


class _FetchSource:
    def __init__(self, payload):
        self._payload = payload

    def fetch(self):
        return self._payload


class _FailingFetchSource:
    def fetch(self):
        raise RuntimeError("boom")


class _FailingSyncSource:
    def __iter__(self):
        yield {"ok": True}
        raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_iter_source_sync_yields_mapping():
    out = []
    async def _run():
        async for raw in iter_source(_SyncSource(), logger=logging.getLogger("test.sync")):
            out.append(raw)
    await asyncio.wait_for(_run(), timeout=1.0)
    assert out == [{"a": 1}]


@pytest.mark.asyncio
async def test_iter_source_fetch_yields_mapping():
    out = []
    async def _run():
        async for raw in iter_source(_FetchSource({"b": 2}), logger=logging.getLogger("test.fetch")):
            out.append(raw)
    await asyncio.wait_for(_run(), timeout=1.0)
    assert out == [{"b": 2}]


@pytest.mark.asyncio
async def test_iter_source_fetch_logs_exception(caplog):
    logger = logging.getLogger("test.fetch.error")
    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError):
            async def _run():
                async for _ in iter_source(
                    _FailingFetchSource(),
                    logger=logger,
                    context={"source": "fetch"},
                    log_exceptions=True,
                ):
                    pass
            await asyncio.wait_for(_run(), timeout=1.0)
    assert any("ingestion.fetch_error" in rec.getMessage() for rec in caplog.records)


@pytest.mark.asyncio
async def test_iter_source_sync_logs_exception(caplog):
    logger = logging.getLogger("test.sync.error")
    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError):
            async def _run():
                async for _ in iter_source(
                    _FailingSyncSource(),
                    logger=logger,
                    context={"source": "sync"},
                    log_exceptions=True,
                ):
                    pass
            await asyncio.wait_for(_run(), timeout=1.0)
    assert any("ingestion.sync_iter_error" in rec.getMessage() for rec in caplog.records)
