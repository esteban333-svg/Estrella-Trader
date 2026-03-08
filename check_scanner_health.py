from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


def _read_health(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        return {}
    return {}


def _parse_iso_utc(value: str) -> datetime | None:
    try:
        return datetime.strptime(str(value or "").strip(), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def main() -> int:
    parser = argparse.ArgumentParser(description="Valida salud operativa de scanner_health.json")
    parser.add_argument("--health", default="scanner_health.json", help="Ruta del archivo de salud.")
    parser.add_argument("--max-stale-sec", type=int, default=240, help="Maximo en segundos sin heartbeat.")
    parser.add_argument(
        "--max-failed-rate-pct",
        type=float,
        default=35.0,
        help="Porcentaje maximo de ciclos fallidos permitido.",
    )
    args = parser.parse_args()

    path = Path(args.health).resolve()
    health = _read_health(path)
    if not health:
        print(f"UNHEALTHY: sin datos de salud en {path}")
        return 1

    now = datetime.now(timezone.utc)
    heartbeat = _parse_iso_utc(health.get("last_heartbeat_utc", ""))
    stale_sec = (now - heartbeat).total_seconds() if heartbeat else float("inf")

    counters = health.get("counters", {})
    if not isinstance(counters, dict):
        counters = {}
    total_cycles = max(0, _safe_int(counters.get("cycles_total", 0), 0))
    failed_cycles = max(0, _safe_int(counters.get("cycles_failed", 0), 0))
    failed_rate = (failed_cycles / total_cycles * 100.0) if total_cycles > 0 else 0.0

    status = str(health.get("status", "unknown")).strip().lower()
    cycle_latency = _safe_float(
        (health.get("latency_ms", {}) or {}).get("cycle", {}).get("avg_ms", 0.0),
        0.0,
    )
    alerts_sent = _safe_int(counters.get("alerts_sent", 0), 0)
    alerts_failed = _safe_int(counters.get("alerts_failed", 0), 0)

    reasons = []
    if stale_sec > max(10, int(args.max_stale_sec)):
        reasons.append(f"heartbeat_stale={stale_sec:.0f}s")
    if total_cycles >= 5 and failed_rate > max(0.0, float(args.max_failed_rate_pct)):
        reasons.append(f"failed_rate={failed_rate:.1f}%")
    if status in {"error", "degraded"} and failed_rate > 0:
        reasons.append(f"status={status}")

    summary = (
        f"status={status} cycles={total_cycles} failed={failed_cycles} "
        f"failed_rate={failed_rate:.1f}% stale_sec={stale_sec:.0f} "
        f"cycle_avg_ms={cycle_latency:.1f} alerts_sent={alerts_sent} alerts_failed={alerts_failed}"
    )
    if reasons:
        print(f"UNHEALTHY: {summary} reasons={','.join(reasons)}")
        return 1

    print(f"HEALTHY: {summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

