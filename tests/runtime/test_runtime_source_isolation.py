from __future__ import annotations

from pathlib import Path


def test_runtime_modules_do_not_import_ingestion_sources() -> None:
    runtime_dir = Path(__file__).resolve().parents[2] / "src" / "quant_engine" / "runtime"
    offenders: list[str] = []
    for path in runtime_dir.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            if "ingestion." in line and ".source" in line:
                offenders.append(f"{path}:{line.strip()}")
                break
    assert not offenders, f"runtime imports ingestion sources:\n" + "\n".join(offenders)
