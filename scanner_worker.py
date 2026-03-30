from __future__ import annotations

import argparse
import ctypes
import gc
import hashlib
import json
import logging
import os
import re
import smtplib
import subprocess
import time
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import pytz
import requests

from analysis import (
    calcular_indicadores,
    construir_estado_final,
    construir_estado_final_estructural,
    obtener_datos,
)
from live_binance import fetch_klines


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = ROOT / "scanner_config.json"
DEFAULT_STATE_PATH = ROOT / "scanner_state.json"
DEFAULT_LOG_PATH = ROOT / "scanner.log"
DEFAULT_HEALTH_PATH = ROOT / "scanner_health.json"
USERS_DB_PATH = Path(os.getenv("USERS_DB_PATH", str(ROOT / "usuarios_db.json"))).resolve()
LOCK_PATH = ROOT / "scanner_worker.lock"
TELEGRAM_AUTO_CHAT_IDS_KEY = "telegram_auto_chat_ids"
TELEGRAM_LAST_UPDATE_ID_KEY = "telegram_last_update_id"
TELEGRAM_USER_CHAT_LINKS_KEY = "telegram_user_chat_links"

NY_TZ = pytz.timezone("America/New_York")
BOGOTA_TZ = pytz.timezone("America/Bogota")

OPERATIONAL_SL_MIN_PCT = 0.5
OPERATIONAL_SL_MAX_PCT = 1.0
OPERATIONAL_TP_R_MULT = 2.0

TD_INTERVAL_MAP = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1day",
}

CRYPTO_MAP = {
    "BTC": {"ticker": "BTC-USD", "binance": "BTCUSDT", "td": "BTC/USD"},
    "ETH": {"ticker": "ETH-USD", "binance": "ETHUSDT", "td": "ETH/USD"},
    "SOL": {"ticker": "SOL-USD", "binance": "SOLUSDT", "td": "SOL/USD"},
    "BNB": {"ticker": "BNB-USD", "binance": "BNBUSDT", "td": "BNB/USD"},
    "XRP": {"ticker": "XRP-USD", "binance": "XRPUSDT", "td": "XRP/USD"},
    "ADA": {"ticker": "ADA-USD", "binance": "ADAUSDT", "td": "ADA/USD"},
    "DOGE": {"ticker": "DOGE-USD", "binance": "DOGEUSDT", "td": "DOGE/USD"},
    "WLD": {"ticker": "WLD-USD", "binance": "WLDUSDT", "td": "WLD/USD"},
}

GOLD_MAP = {
    "XAU/USD": {"ticker": "GC=F", "td": "XAU/USD"},
}

CRYPTO_COINMARKETCAP_IDS = {
    "BTC": 1,
    "ETH": 1027,
    "SOL": 5426,
    "BNB": 1839,
    "XRP": 52,
    "ADA": 2010,
    "DOGE": 74,
    "WLD": 24091,
}

SENSITIVE_ENV_KEYS = [
    "TELEGRAM_BOT_TOKEN",
    "SMTP_PASSWORD",
    "SMTP_USER",
    "SMTP_FROM",
    "AUTH_COOKIE_PASSWORD",
    "PREMIUM_ACCESS_CODE",
]

TOKEN_PATTERN = re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b")
EMAIL_PATTERN = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
CHAT_ID_PATTERN = re.compile(r"(?:(?<=^)|(?<=[\s,;]))-?\d{8,}(?=\s*:)")
TELEGRAM_START_CODE_PATTERN = re.compile(r"\bet[a-f0-9]{16}\b")


@dataclass(frozen=True)
class MarketItem:
    market: str
    label: str
    ticker: str
    td_symbol: str
    kind: str  # "crypto" | "gold"
    binance_symbol: str = ""

    @property
    def state_key(self) -> str:
        return f"{self.market}|{self.label}|{self.ticker}"


def _sensitive_env_values() -> List[str]:
    values: List[str] = []
    for key in SENSITIVE_ENV_KEYS:
        raw = str(os.getenv(key, "")).strip()
        if len(raw) >= 4:
            values.append(raw)
    return values


def _redact_text(value: Any) -> str:
    text = str(value or "")
    if not text:
        return text

    for secret in _sensitive_env_values():
        if secret and secret in text:
            text = text.replace(secret, "***REDACTED***")

    text = TOKEN_PATTERN.sub("<redacted-token>", text)
    text = CHAT_ID_PATTERN.sub("<redacted-chat-id>", text)
    text = EMAIL_PATTERN.sub("<redacted-email>", text)
    return text


class _SecretRedactionFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if isinstance(record.msg, str):
                record.msg = _redact_text(record.msg)
            if isinstance(record.args, tuple):
                record.args = tuple(_redact_text(arg) if isinstance(arg, str) else arg for arg in record.args)
            elif isinstance(record.args, dict):
                record.args = {k: (_redact_text(v) if isinstance(v, str) else v) for k, v in record.args.items()}
        except Exception:
            return True
        return True


def _resolve_resource_profile(cfg: Dict[str, Any]) -> str:
    env_profile = str(os.getenv("SCANNER_RESOURCE_PROFILE", "")).strip().lower()
    if env_profile:
        return env_profile
    cfg_profile = str(cfg.get("resource_profile", "")).strip().lower()
    if cfg_profile:
        return cfg_profile
    if os.getenv("RENDER", "").strip().lower() in {"1", "true", "yes"}:
        return "render_512mb"
    return "default"


def _resource_profile_active(cfg: Dict[str, Any], profile_name: str) -> bool:
    return _resolve_resource_profile(cfg) == profile_name


def _current_process_rss_mb() -> float:
    try:
        if os.name == "nt":
            # Windows: psapi via tasklist fallback avoids extra dependencies.
            pid = os.getpid()
            proc = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                timeout=4,
                check=False,
            )
            out = (proc.stdout or "").strip()
            if not out or out.startswith("INFO:"):
                return 0.0
            parts = [p.strip().strip('"') for p in out.split(",")]
            # Mem usage field e.g. "123,456 K"
            if len(parts) >= 5:
                mem_text = parts[4].replace(".", "").replace(",", "").replace("K", "").strip()
                kb = float(mem_text)
                return round(kb / 1024.0, 2)
            return 0.0

        # Linux/macOS
        statm = Path("/proc/self/statm")
        if statm.exists():
            raw = statm.read_text(encoding="utf-8").strip().split()
            if len(raw) >= 2:
                rss_pages = float(raw[1])
                page_size = float(os.sysconf("SC_PAGE_SIZE"))
                return round((rss_pages * page_size) / (1024.0 * 1024.0), 2)
    except Exception:
        return 0.0
    return 0.0


def _trim_process_memory() -> None:
    try:
        if os.name != "posix":
            return
        libc = ctypes.CDLL("libc.so.6")
        malloc_trim = getattr(libc, "malloc_trim", None)
        if malloc_trim is None:
            return
        malloc_trim(0)
    except Exception:
        return


def _compact_memory_sweep() -> None:
    gc.collect()
    _trim_process_memory()


def _trim_df_for_interval(df: pd.DataFrame | None, interval: str, cfg: Dict[str, Any]) -> pd.DataFrame | None:
    if df is None or df.empty:
        return df

    max_rows_default = 720
    max_rows_map = {
        "1m": 900,
        "5m": 800,
        "15m": 720,
        "30m": 640,
        "1h": 560,
        "4h": 520,
        "1d": 500,
    }
    max_rows = max_rows_map.get(str(interval).strip().lower(), max_rows_default)

    raw_cfg = cfg.get("runtime_limits", {})
    if isinstance(raw_cfg, dict):
        try:
            max_rows = int(raw_cfg.get("max_rows_default", max_rows))
        except Exception:
            pass
        interval_map = raw_cfg.get("max_rows_by_interval", {})
        if isinstance(interval_map, dict):
            try:
                max_rows = int(interval_map.get(str(interval).strip().lower(), max_rows))
            except Exception:
                pass

    if _resource_profile_active(cfg, "render_512mb"):
        max_rows = min(max_rows, 540)

    if max_rows <= 0:
        return df
    if len(df) <= max_rows:
        return df
    return df.tail(max_rows).copy()


def _apply_resource_profile(cfg: Dict[str, Any]) -> Dict[str, Any]:
    profile = _resolve_resource_profile(cfg)
    if profile != "render_512mb":
        return cfg

    tuned = dict(cfg)
    tuned["resource_profile"] = "render_512mb"
    # In 512MB, structural + many intervals aumentan RAM/latencia.
    tuned["scan_structural_1d_4h"] = False
    tuned["auto_multi_interval"] = True
    tuned["scan_intervals"] = ["15m", "1h"]
    tuned["poll_interval_sec"] = max(90, int(tuned.get("poll_interval_sec", 60)))

    crypto = tuned.get("crypto_symbols", [])
    if isinstance(crypto, list) and len(crypto) > 4:
        tuned["crypto_symbols"] = crypto[:4]
    gold = tuned.get("gold_symbols", [])
    if isinstance(gold, list) and len(gold) > 1:
        tuned["gold_symbols"] = gold[:1]

    runtime_limits = tuned.get("runtime_limits", {})
    if not isinstance(runtime_limits, dict):
        runtime_limits = {}
    runtime_limits["max_rows_default"] = min(540, int(runtime_limits.get("max_rows_default", 720)))
    runtime_limits["max_rows_by_interval"] = {
        "1m": 600,
        "5m": 560,
        "15m": 540,
        "30m": 520,
        "1h": 500,
        "4h": 420,
        "1d": 360,
    }
    tuned["runtime_limits"] = runtime_limits
    return tuned


def _default_config() -> Dict[str, Any]:
    return {
        "enabled": True,
        "poll_interval_sec": 60,
        "analysis_mode": "tendencial",
        "period": "5d",
        "interval": "15m",
        "auto_multi_interval": True,
        "scan_intervals": ["15m", "30m", "1h", "4h"],
        "scan_structural_1d_4h": True,
        "cooldown_minutes": 60,
        "scan_crypto": True,
        "scan_gold": True,
        "resource_profile": "default",
        "runtime_limits": {
            "max_rows_default": 720,
            "max_rows_by_interval": {
                "1m": 900,
                "5m": 800,
                "15m": 720,
                "30m": 640,
                "1h": 560,
                "4h": 520,
                "1d": 500,
            },
        },
        "precision_filters": {
            "enabled": True,
            "alert_profile": "balanceado",
            "require_closed_candle": True,
            "closed_candle_grace_sec": 10,
            "persistence_bars": 2,
            "multi_timeframe_filter": True,
            "require_price_action_confirmation": True,
            "min_confidence_score": 85,
            "min_rr": 1.8,
            "adaptive_threshold": True,
            "adaptive_cooldown": True,
            "quality_calibration_enabled": True,
            "quality_calibration_min_resolved": 20,
            "quality_calibration_scope": "global_and_record",
            "quality_calibration_record_enabled": True,
            "quality_calibration_record_min_resolved": 8,
            "max_alerts_per_symbol_day": 2,
            "min_mtf_confirmations": 2,
            "quality_window_bars": 12,
            "quality_window_bars_by_interval": {
                "5m": 16,
                "15m": 12,
                "30m": 10,
                "1h": 8,
                "4h": 6,
                "1d": 5,
            },
            "mtf_intervals": {
                "15m": ["30m", "1h", "4h", "1d"],
                "30m": ["1h", "4h", "1d"],
                "1h": ["4h", "1d"],
            },
            "base_cooldown_by_interval": {
                "5m": 35,
                "15m": 45,
                "30m": 60,
                "1h": 90,
                "4h": 180,
                "1d": 720,
            },
        },
        "crypto_symbols": list(CRYPTO_MAP.keys()),
        "gold_symbols": list(GOLD_MAP.keys()),
        "notification": {
            "subject_prefix": "",
            "email": {
                "enabled": True,
                "to": [],
            },
            "telegram": {
                "enabled": True,
                "chat_ids": [],
                "parse_mode": "",
                "send_coin_image": True,
                "coin_image_urls": {},
            },
            "windows": {
                "enabled": True,
            },
        },
    }


def _setup_logging(log_path: Path, debug: bool) -> None:
    log_level = logging.DEBUG if debug else logging.INFO
    try:
        max_mb = max(1, int(os.getenv("SCANNER_LOG_MAX_MB", "20")))
    except Exception:
        max_mb = 20
    try:
        backup_count = max(1, int(os.getenv("SCANNER_LOG_BACKUP_COUNT", "5")))
    except Exception:
        backup_count = 5
    secret_filter = _SecretRedactionFilter()
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=max_mb * 1024 * 1024,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.addFilter(secret_filter)
    stream_handler = logging.StreamHandler()
    stream_handler.addFilter(secret_filter)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            file_handler,
            stream_handler,
        ],
    )


def _pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            proc = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            out = (proc.stdout or "").strip()
            if not out:
                return False
            if "No tasks are running" in out or out.startswith("INFO:"):
                return False
            return str(pid) in out
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def _read_lock_pid(lock_path: Path) -> int:
    try:
        raw = lock_path.read_text(encoding="utf-8").strip()
    except Exception:
        return 0
    if not raw:
        return 0
    try:
        payload = json.loads(raw)
        return int(payload.get("pid", 0) or 0)
    except Exception:
        try:
            return int(raw)
        except Exception:
            return 0


def _acquire_single_instance_lock(lock_path: Path) -> Tuple[bool, str]:
    payload = {
        "pid": os.getpid(),
        "started_utc": datetime.now(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    for _ in range(2):
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, ensure_ascii=False))
            return True, ""
        except FileExistsError:
            existing_pid = _read_lock_pid(lock_path)
            if existing_pid > 0 and _pid_running(existing_pid):
                return False, f"Scanner ya en ejecucion (PID={existing_pid})."
            try:
                lock_path.unlink(missing_ok=True)
            except Exception as exc:
                return False, f"No se pudo limpiar lock huerfano: {exc}"
        except Exception as exc:
            return False, f"No se pudo crear lock de instancia unica: {exc}"
    return False, "No se pudo adquirir lock de instancia unica."


def _release_single_instance_lock(lock_path: Path) -> None:
    try:
        existing_pid = _read_lock_pid(lock_path)
        if existing_pid in {0, os.getpid()}:
            lock_path.unlink(missing_ok=True)
    except Exception:
        pass


def _merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


def _is_legacy_forex_key(value: Any) -> bool:
    return str(value or "").strip().startswith("Forex|")


def _sanitize_legacy_forex_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    clean = dict(payload)
    clean.pop("scan_forex", None)
    clean.pop("forex_pairs", None)
    return clean


def _prune_legacy_forex_from_state(state: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(state, dict):
        return {"symbols": {}, "free_daily_market_alerts": {}}

    symbols = state.get("symbols", {})
    if isinstance(symbols, dict):
        state["symbols"] = {
            key: value for key, value in symbols.items()
            if not _is_legacy_forex_key(key)
        }

    free_alerts = state.get("free_daily_market_alerts", {})
    if isinstance(free_alerts, dict):
        clean_free_alerts: Dict[str, Any] = {}
        for user_id, user_state in free_alerts.items():
            if not isinstance(user_state, dict):
                continue
            filtered = {
                key: value for key, value in user_state.items()
                if not _is_legacy_forex_key(key)
            }
            if filtered:
                clean_free_alerts[user_id] = filtered
        state["free_daily_market_alerts"] = clean_free_alerts

    return state


def _prune_legacy_forex_from_health(health: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(health, dict):
        return _default_health_state()

    recent_errors = health.get("recent_errors", [])
    if isinstance(recent_errors, list):
        health["recent_errors"] = [
            err for err in recent_errors
            if not _is_legacy_forex_key((err.get("message", "") if isinstance(err, dict) else err))
        ]
    return health


def _read_json(path: Path, fallback: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if not path.exists():
            return fallback
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        if isinstance(payload, dict):
            return payload
    except Exception as exc:
        logging.error("No se pudo leer %s: %s", path, exc)
    return fallback


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_config(config_path: Path) -> Dict[str, Any]:
    defaults = _default_config()
    if not config_path.exists():
        _write_json(config_path, defaults)
        logging.info("Config creada en %s", config_path)
        return defaults
    user_cfg = _read_json(config_path, {})
    return _merge_dicts(defaults, _sanitize_legacy_forex_config(user_cfg))


def load_state(state_path: Path) -> Dict[str, Any]:
    fallback = {"symbols": {}, "free_daily_market_alerts": {}}
    state = _read_json(state_path, fallback)
    if not isinstance(state.get("symbols"), dict):
        state["symbols"] = {}
    if not isinstance(state.get("free_daily_market_alerts"), dict):
        state["free_daily_market_alerts"] = {}
    if not isinstance(state.get(TELEGRAM_AUTO_CHAT_IDS_KEY), list):
        state[TELEGRAM_AUTO_CHAT_IDS_KEY] = []
    if not isinstance(state.get(TELEGRAM_USER_CHAT_LINKS_KEY), dict):
        state[TELEGRAM_USER_CHAT_LINKS_KEY] = {}
    try:
        state[TELEGRAM_LAST_UPDATE_ID_KEY] = int(state.get(TELEGRAM_LAST_UPDATE_ID_KEY, 0) or 0)
    except Exception:
        state[TELEGRAM_LAST_UPDATE_ID_KEY] = 0
    return _prune_legacy_forex_from_state(state)


def save_state(state_path: Path, state: Dict[str, Any]) -> None:
    _write_json(state_path, state)


def _iso_utc_now() -> str:
    return datetime.now(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_latency_stats() -> Dict[str, Any]:
    return {
        "count": 0,
        "total_ms": 0.0,
        "avg_ms": 0.0,
        "max_ms": 0.0,
        "last_ms": 0.0,
    }


def _observe_latency(stats: Dict[str, Any], elapsed_ms: float) -> None:
    if not isinstance(stats, dict):
        return
    try:
        value = max(0.0, float(elapsed_ms))
    except Exception:
        return

    count = int(stats.get("count", 0) or 0) + 1
    total = float(stats.get("total_ms", 0.0) or 0.0) + value
    max_val = max(float(stats.get("max_ms", 0.0) or 0.0), value)
    stats["count"] = count
    stats["total_ms"] = round(total, 3)
    stats["avg_ms"] = round(total / count, 3) if count > 0 else 0.0
    stats["max_ms"] = round(max_val, 3)
    stats["last_ms"] = round(value, 3)


def _merge_latency_stats(target: Dict[str, Any], sample: Dict[str, Any]) -> None:
    if not isinstance(target, dict) or not isinstance(sample, dict):
        return
    sample_count = int(sample.get("count", 0) or 0)
    if sample_count <= 0:
        return
    sample_total = float(sample.get("total_ms", 0.0) or 0.0)
    if sample_total <= 0:
        sample_total = float(sample.get("avg_ms", 0.0) or 0.0) * sample_count
    sample_max = float(sample.get("max_ms", 0.0) or 0.0)
    sample_last = float(sample.get("last_ms", 0.0) or 0.0)

    target_count = int(target.get("count", 0) or 0)
    target_total = float(target.get("total_ms", 0.0) or 0.0)
    merged_count = target_count + sample_count
    merged_total = target_total + sample_total
    target["count"] = merged_count
    target["total_ms"] = round(merged_total, 3)
    target["avg_ms"] = round(merged_total / merged_count, 3) if merged_count > 0 else 0.0
    target["max_ms"] = round(max(float(target.get("max_ms", 0.0) or 0.0), sample_max), 3)
    target["last_ms"] = round(sample_last, 3)


def _new_channel_health_stats() -> Dict[str, Any]:
    return {
        "attempts": 0,
        "sent": 0,
        "failed": 0,
        "latency_ms": _new_latency_stats(),
    }


def _new_cycle_metrics() -> Dict[str, Any]:
    return {
        "started_utc": _iso_utc_now(),
        "duration_ms": 0.0,
        "rss_start_mb": 0.0,
        "rss_end_mb": 0.0,
        "rss_peak_mb": 0.0,
        "watchlist_size": 0,
        "scan_targets": 0,
        "records_total": 0,
        "records_success": 0,
        "records_error": 0,
        "alerts_triggered": 0,
        "alerts_sent": 0,
        "alerts_failed": 0,
        "latency_ms": {
            "record_eval": _new_latency_stats(),
            "notification": _new_latency_stats(),
        },
        "notifications": {
            "telegram": _new_channel_health_stats(),
            "email": _new_channel_health_stats(),
            "windows": _new_channel_health_stats(),
        },
        "errors": [],
    }


def _default_health_state() -> Dict[str, Any]:
    now_iso = _iso_utc_now()
    return {
        "status": "starting",
        "release": str(os.getenv("RELEASE_VERSION", os.getenv("RENDER_GIT_COMMIT", "local"))).strip() or "local",
        "pid": os.getpid(),
        "started_utc": now_iso,
        "started_epoch": time.time(),
        "last_heartbeat_utc": now_iso,
        "last_cycle_utc": "",
        "last_success_utc": "",
        "last_error_utc": "",
        "last_error": "",
        "uptime_sec": 0,
        "rss_mb": 0.0,
        "rss_peak_cycle_mb": 0.0,
        "paths": {
            "config": "",
            "state": "",
            "log": "",
        },
        "counters": {
            "cycles_total": 0,
            "cycles_ok": 0,
            "cycles_failed": 0,
            "records_total": 0,
            "records_error": 0,
            "alerts_triggered": 0,
            "alerts_sent": 0,
            "alerts_failed": 0,
            "errors_total": 0,
        },
        "latency_ms": {
            "cycle": _new_latency_stats(),
            "record_eval": _new_latency_stats(),
            "notification": _new_latency_stats(),
        },
        "notifications": {
            "telegram": _new_channel_health_stats(),
            "email": _new_channel_health_stats(),
            "windows": _new_channel_health_stats(),
        },
        "last_cycle": {},
        "recent_errors": [],
    }


def _ensure_health_shape(health: Dict[str, Any] | None) -> Dict[str, Any]:
    base = _default_health_state()
    if not isinstance(health, dict):
        return base
    return _merge_dicts(base, health)


def load_health(health_path: Path) -> Dict[str, Any]:
    return _prune_legacy_forex_from_health(_ensure_health_shape(_read_json(health_path, _default_health_state())))


def save_health(health_path: Path, health: Dict[str, Any]) -> None:
    _write_json(health_path, health)


def _record_notification_metrics(cycle_metrics: Dict[str, Any], channel: str, sent_ok: bool, elapsed_ms: float) -> None:
    notifications = cycle_metrics.get("notifications", {})
    if not isinstance(notifications, dict):
        return
    bucket = notifications.get(channel)
    if not isinstance(bucket, dict):
        return
    bucket["attempts"] = int(bucket.get("attempts", 0) or 0) + 1
    if sent_ok:
        bucket["sent"] = int(bucket.get("sent", 0) or 0) + 1
    else:
        bucket["failed"] = int(bucket.get("failed", 0) or 0) + 1
    latency = bucket.get("latency_ms", {})
    if isinstance(latency, dict):
        _observe_latency(latency, elapsed_ms)
    global_latency = cycle_metrics.get("latency_ms", {}).get("notification", {})
    if isinstance(global_latency, dict):
        _observe_latency(global_latency, elapsed_ms)


def _update_health_from_cycle(
    health: Dict[str, Any],
    cycle_metrics: Dict[str, Any],
    cycle_ok: bool,
    cycle_error: str = "",
) -> Dict[str, Any]:
    payload = _ensure_health_shape(health)
    now_iso = _iso_utc_now()
    payload["pid"] = os.getpid()
    payload["release"] = str(
        os.getenv("RELEASE_VERSION", os.getenv("RENDER_GIT_COMMIT", payload.get("release", "local")))
    ).strip() or "local"
    payload["last_heartbeat_utc"] = now_iso
    payload["last_cycle_utc"] = now_iso
    payload["status"] = "ok" if cycle_ok else "degraded"
    rss_end = float(cycle_metrics.get("rss_end_mb", 0.0) or 0.0)
    rss_peak_cycle = float(cycle_metrics.get("rss_peak_mb", rss_end) or rss_end)
    if rss_end > 0:
        payload["rss_mb"] = round(rss_end, 2)
    if rss_peak_cycle > 0:
        payload["rss_peak_cycle_mb"] = round(rss_peak_cycle, 2)
    if cycle_ok:
        payload["last_success_utc"] = now_iso
    else:
        payload["last_error_utc"] = now_iso
        payload["last_error"] = _redact_text(str(cycle_error or "Error no controlado en ciclo."))

    try:
        started_epoch = float(payload.get("started_epoch", time.time()) or time.time())
    except Exception:
        started_epoch = time.time()
    payload["uptime_sec"] = max(0, int(time.time() - started_epoch))

    counters = payload.get("counters", {})
    if not isinstance(counters, dict):
        counters = {}
        payload["counters"] = counters
    counters["cycles_total"] = int(counters.get("cycles_total", 0) or 0) + 1
    if cycle_ok:
        counters["cycles_ok"] = int(counters.get("cycles_ok", 0) or 0) + 1
    else:
        counters["cycles_failed"] = int(counters.get("cycles_failed", 0) or 0) + 1
        counters["errors_total"] = int(counters.get("errors_total", 0) or 0) + 1

    counters["records_total"] = int(counters.get("records_total", 0) or 0) + int(cycle_metrics.get("records_total", 0) or 0)
    counters["records_error"] = int(counters.get("records_error", 0) or 0) + int(cycle_metrics.get("records_error", 0) or 0)
    counters["alerts_triggered"] = int(counters.get("alerts_triggered", 0) or 0) + int(cycle_metrics.get("alerts_triggered", 0) or 0)
    counters["alerts_sent"] = int(counters.get("alerts_sent", 0) or 0) + int(cycle_metrics.get("alerts_sent", 0) or 0)
    counters["alerts_failed"] = int(counters.get("alerts_failed", 0) or 0) + int(cycle_metrics.get("alerts_failed", 0) or 0)

    latencies = payload.get("latency_ms", {})
    if not isinstance(latencies, dict):
        latencies = {}
        payload["latency_ms"] = latencies
    for key in ("cycle", "record_eval", "notification"):
        if not isinstance(latencies.get(key), dict):
            latencies[key] = _new_latency_stats()

    cycle_latency = {
        "count": 1,
        "total_ms": float(cycle_metrics.get("duration_ms", 0.0) or 0.0),
        "avg_ms": float(cycle_metrics.get("duration_ms", 0.0) or 0.0),
        "max_ms": float(cycle_metrics.get("duration_ms", 0.0) or 0.0),
        "last_ms": float(cycle_metrics.get("duration_ms", 0.0) or 0.0),
    }
    _merge_latency_stats(latencies["cycle"], cycle_latency)
    _merge_latency_stats(latencies["record_eval"], cycle_metrics.get("latency_ms", {}).get("record_eval", {}))
    _merge_latency_stats(latencies["notification"], cycle_metrics.get("latency_ms", {}).get("notification", {}))

    notif = payload.get("notifications", {})
    if not isinstance(notif, dict):
        notif = {}
        payload["notifications"] = notif
    for channel in ("telegram", "email", "windows"):
        if not isinstance(notif.get(channel), dict):
            notif[channel] = _new_channel_health_stats()
        channel_target = notif[channel]
        channel_sample = cycle_metrics.get("notifications", {}).get(channel, {})
        channel_target["attempts"] = int(channel_target.get("attempts", 0) or 0) + int(channel_sample.get("attempts", 0) or 0)
        channel_target["sent"] = int(channel_target.get("sent", 0) or 0) + int(channel_sample.get("sent", 0) or 0)
        channel_target["failed"] = int(channel_target.get("failed", 0) or 0) + int(channel_sample.get("failed", 0) or 0)
        _merge_latency_stats(channel_target.get("latency_ms", {}), channel_sample.get("latency_ms", {}))

    last_cycle = {
        "started_utc": str(cycle_metrics.get("started_utc", "")),
        "duration_ms": round(float(cycle_metrics.get("duration_ms", 0.0) or 0.0), 3),
        "watchlist_size": int(cycle_metrics.get("watchlist_size", 0) or 0),
        "scan_targets": int(cycle_metrics.get("scan_targets", 0) or 0),
        "records_total": int(cycle_metrics.get("records_total", 0) or 0),
        "records_success": int(cycle_metrics.get("records_success", 0) or 0),
        "records_error": int(cycle_metrics.get("records_error", 0) or 0),
        "alerts_triggered": int(cycle_metrics.get("alerts_triggered", 0) or 0),
        "alerts_sent": int(cycle_metrics.get("alerts_sent", 0) or 0),
        "alerts_failed": int(cycle_metrics.get("alerts_failed", 0) or 0),
        "rss_start_mb": round(float(cycle_metrics.get("rss_start_mb", 0.0) or 0.0), 2),
        "rss_end_mb": round(float(cycle_metrics.get("rss_end_mb", 0.0) or 0.0), 2),
        "rss_peak_mb": round(float(cycle_metrics.get("rss_peak_mb", 0.0) or 0.0), 2),
        "cycle_ok": bool(cycle_ok),
    }
    payload["last_cycle"] = last_cycle

    errors = payload.get("recent_errors", [])
    if not isinstance(errors, list):
        errors = []
    cycle_errors = cycle_metrics.get("errors", [])
    if not isinstance(cycle_errors, list):
        cycle_errors = []
    for raw in cycle_errors:
        msg = _redact_text(str(raw or "").strip())
        if not msg:
            continue
        errors.append({"at_utc": now_iso, "message": msg})
    if cycle_error:
        errors.append({"at_utc": now_iso, "message": _redact_text(str(cycle_error))})
    payload["recent_errors"] = errors[-30:]
    return payload


def _parse_recipients(cfg: Dict[str, Any]) -> List[str]:
    recipients: List[str] = []

    notif = cfg.get("notification", {}) if isinstance(cfg.get("notification", {}), dict) else {}

    # Legacy key support: notification.email_to
    from_cfg_legacy = notif.get("email_to", [])
    if isinstance(from_cfg_legacy, list):
        recipients.extend([str(x).strip() for x in from_cfg_legacy if str(x).strip()])

    from_cfg = (notif.get("email", {}) or {}).get("to", [])
    if isinstance(from_cfg, list):
        recipients.extend([str(x).strip() for x in from_cfg if str(x).strip()])

    from_env = os.getenv("ALERT_EMAIL_TO", "").strip()
    if from_env:
        recipients.extend([p.strip() for p in from_env.split(",") if p.strip()])

    # Remove duplicates preserving order.
    seen = set()
    clean = []
    for email in recipients:
        low = email.lower()
        if low in seen:
            continue
        seen.add(low)
        clean.append(email)
    return clean


def _parse_telegram_chat_ids(cfg: Dict[str, Any]) -> List[str]:
    ids: List[str] = []
    notif = cfg.get("notification", {}) if isinstance(cfg.get("notification", {}), dict) else {}
    from_cfg = (notif.get("telegram", {}) or {}).get("chat_ids", [])
    if isinstance(from_cfg, list):
        ids.extend([str(x).strip() for x in from_cfg if str(x).strip()])

    from_env = os.getenv("ALERT_TELEGRAM_CHAT_IDS", "").strip()
    if from_env:
        ids.extend([x.strip() for x in from_env.split(",") if x.strip()])

    seen = set()
    clean = []
    for cid in ids:
        if cid in seen:
            continue
        seen.add(cid)
        clean.append(cid)
    return clean


def _normalizar_telegram_chat_id(chat_id: str) -> str:
    return str(chat_id or "").strip()


def _chat_id_telegram_valido(chat_id: str) -> bool:
    cid = _normalizar_telegram_chat_id(chat_id)
    if not cid:
        return False
    if cid.startswith("-"):
        return cid[1:].isdigit()
    return cid.isdigit()


def _dedupe_chat_ids(chat_ids: List[str]) -> List[str]:
    seen = set()
    clean: List[str] = []
    for raw in chat_ids:
        cid = _normalizar_telegram_chat_id(raw)
        if not _chat_id_telegram_valido(cid):
            continue
        if cid in seen:
            continue
        seen.add(cid)
        clean.append(cid)
    return clean


def _auth_cookie_password() -> str:
    auth_secret = str(os.getenv("AUTH_COOKIE_PASSWORD", "") or "").strip()
    if auth_secret:
        return auth_secret

    host_name = os.getenv("COMPUTERNAME", os.getenv("HOSTNAME", "local"))
    app_path = os.path.abspath(str(ROOT / "app.py"))
    seed = f"{host_name}|{app_path}|estrella-auth-fallback"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _telegram_start_code_usuario(user_id: str) -> str:
    uid = str(user_id or "").strip().lower()
    auth_secret = _auth_cookie_password()
    if not uid or not auth_secret:
        return ""
    raw = f"{uid}|{auth_secret}|telegram-link"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"et{digest[:16]}"


def _extract_start_links_from_update(update: Dict[str, Any]) -> List[Tuple[str, str]]:
    links: List[Tuple[str, str]] = []
    if not isinstance(update, dict):
        return links

    for key in ("message", "edited_message", "channel_post", "edited_channel_post"):
        payload = update.get(key, {})
        if not isinstance(payload, dict):
            continue
        chat = payload.get("chat", {})
        if not isinstance(chat, dict):
            continue
        chat_id = _normalizar_telegram_chat_id(chat.get("id", ""))
        if not _chat_id_telegram_valido(chat_id):
            continue

        text = str(payload.get("text", "") or payload.get("caption", "")).strip().lower()
        if not text:
            continue

        for match in TELEGRAM_START_CODE_PATTERN.findall(text):
            links.append((match, chat_id))

    return links


def _normalize_telegram_user_chat_links(state: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    raw_links = state.get(TELEGRAM_USER_CHAT_LINKS_KEY, {})
    if not isinstance(raw_links, dict):
        state[TELEGRAM_USER_CHAT_LINKS_KEY] = {}
        return {}

    clean_links: Dict[str, Dict[str, str]] = {}
    for raw_user_id, raw_payload in raw_links.items():
        user_id = str(raw_user_id or "").strip()
        if not user_id:
            continue

        chat_id = ""
        updated_utc = ""
        if isinstance(raw_payload, dict):
            chat_id = _normalizar_telegram_chat_id(raw_payload.get("chat_id", ""))
            updated_utc = str(raw_payload.get("updated_utc", "") or "").strip()
        else:
            chat_id = _normalizar_telegram_chat_id(raw_payload)

        if not _chat_id_telegram_valido(chat_id):
            continue

        clean_links[user_id] = {
            "chat_id": chat_id,
            "updated_utc": updated_utc,
            "source": "telegram_start",
        }

    state[TELEGRAM_USER_CHAT_LINKS_KEY] = clean_links
    return clean_links


def _coin_image_url_for_telegram(cfg: Dict[str, Any], item: MarketItem | None) -> str:
    if item is None or str(item.kind).lower() != "crypto":
        return ""

    notif = cfg.get("notification", {}) if isinstance(cfg.get("notification", {}), dict) else {}
    tg_cfg_raw = notif.get("telegram", {})
    tg_cfg = tg_cfg_raw if isinstance(tg_cfg_raw, dict) else {}

    if not bool(tg_cfg.get("send_coin_image", True)):
        return ""

    symbol = str(item.label or "").strip().upper()
    custom_urls = tg_cfg.get("coin_image_urls", {})
    if isinstance(custom_urls, dict):
        custom = str(custom_urls.get(symbol, "") or custom_urls.get(symbol.lower(), "")).strip()
        if custom:
            return custom

    cmc_id = CRYPTO_COINMARKETCAP_IDS.get(symbol)
    if not cmc_id:
        return ""
    return f"https://s2.coinmarketcap.com/static/img/coins/64x64/{cmc_id}.png"


def _extract_chat_ids_from_update(update: Dict[str, Any]) -> List[str]:
    ids: List[str] = []
    if not isinstance(update, dict):
        return ids

    def _push_chat_id(value: Any):
        cid = _normalizar_telegram_chat_id(str(value or ""))
        if _chat_id_telegram_valido(cid):
            ids.append(cid)

    for key in ("message", "edited_message", "channel_post", "edited_channel_post", "my_chat_member", "chat_member"):
        payload = update.get(key, {})
        if isinstance(payload, dict):
            chat = payload.get("chat", {})
            if isinstance(chat, dict):
                _push_chat_id(chat.get("id"))

    callback = update.get("callback_query", {})
    if isinstance(callback, dict):
        msg = callback.get("message", {})
        if isinstance(msg, dict):
            chat = msg.get("chat", {})
            if isinstance(chat, dict):
                _push_chat_id(chat.get("id"))

    return _dedupe_chat_ids(ids)


def _discover_telegram_chats_from_updates(
    state: Dict[str, Any],
    user_records: List[Dict[str, Any]] | None = None,
) -> Tuple[List[str], Dict[str, Dict[str, str]], str]:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    known_ids = _dedupe_chat_ids(state.get(TELEGRAM_AUTO_CHAT_IDS_KEY, []))
    user_chat_links = _normalize_telegram_user_chat_links(state)
    start_code_map: Dict[str, str] = {}
    for raw_user in user_records or []:
        if not isinstance(raw_user, dict):
            continue
        user_id = str(raw_user.get("id", "") or "").strip()
        if not user_id:
            continue
        start_code = _telegram_start_code_usuario(user_id)
        if start_code:
            start_code_map[start_code] = user_id

    if not token:
        state[TELEGRAM_AUTO_CHAT_IDS_KEY] = known_ids
        state[TELEGRAM_USER_CHAT_LINKS_KEY] = user_chat_links
        return known_ids, user_chat_links, "TELEGRAM_BOT_TOKEN no configurado."

    try:
        last_update_id = int(state.get(TELEGRAM_LAST_UPDATE_ID_KEY, 0) or 0)
    except Exception:
        last_update_id = 0

    params: Dict[str, Any] = {"timeout": 0}
    if last_update_id > 0:
        params["offset"] = last_update_id + 1

    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{token}/getUpdates",
            params=params,
            timeout=20,
        )
        if resp.status_code >= 400:
            state[TELEGRAM_AUTO_CHAT_IDS_KEY] = known_ids
            state[TELEGRAM_USER_CHAT_LINKS_KEY] = user_chat_links
            return known_ids, user_chat_links, _redact_text(f"HTTP {resp.status_code} al consultar getUpdates.")
        payload = resp.json()
        if not payload.get("ok", False):
            state[TELEGRAM_AUTO_CHAT_IDS_KEY] = known_ids
            state[TELEGRAM_USER_CHAT_LINKS_KEY] = user_chat_links
            return known_ids, user_chat_links, _redact_text(payload.get("description", "Error getUpdates sin descripcion."))
        updates = payload.get("result", [])
        if not isinstance(updates, list):
            updates = []
    except Exception as exc:
        state[TELEGRAM_AUTO_CHAT_IDS_KEY] = known_ids
        state[TELEGRAM_USER_CHAT_LINKS_KEY] = user_chat_links
        return known_ids, user_chat_links, _redact_text(str(exc))

    max_update_id = last_update_id
    merged = list(known_ids)
    seen = set(merged)
    added = 0
    linked = 0
    for upd in updates:
        if not isinstance(upd, dict):
            continue
        try:
            upd_id = int(upd.get("update_id", 0) or 0)
        except Exception:
            upd_id = 0
        if upd_id > max_update_id:
            max_update_id = upd_id

        for cid in _extract_chat_ids_from_update(upd):
            if cid in seen:
                continue
            seen.add(cid)
            merged.append(cid)
            added += 1

        for start_code, chat_id in _extract_start_links_from_update(upd):
            user_id = start_code_map.get(start_code, "")
            if not user_id:
                continue
            current = user_chat_links.get(user_id, {}) if isinstance(user_chat_links.get(user_id, {}), dict) else {}
            if str(current.get("chat_id", "") or "").strip() == chat_id:
                continue
            user_chat_links[user_id] = {
                "chat_id": chat_id,
                "updated_utc": _iso_utc_now(),
                "source": "telegram_start",
            }
            linked += 1

    state[TELEGRAM_AUTO_CHAT_IDS_KEY] = merged
    state[TELEGRAM_LAST_UPDATE_ID_KEY] = max_update_id
    state[TELEGRAM_USER_CHAT_LINKS_KEY] = user_chat_links
    if added > 0:
        logging.info("Telegram: %s chat(s) nuevos detectados via getUpdates.", added)
    if linked > 0:
        logging.info("Telegram: %s vinculo(s) user/chat actualizados via /start.", linked)
    return merged, user_chat_links, ""


def _premium_activo_usuario(record: Dict[str, Any]) -> bool:
    if not bool(record.get("es_premium", False)):
        return False
    until = str(record.get("premium_until", "") or "").strip()
    if not until:
        return True
    until_dt = _parse_iso_utc(until)
    if until_dt is None:
        return bool(record.get("es_premium", False))
    return datetime.now(pytz.UTC) <= until_dt


def _load_user_records(users_db_path: Path = USERS_DB_PATH) -> List[Dict[str, Any]]:
    payload = _read_json(users_db_path, {"users": []})
    users = payload.get("users") if isinstance(payload, dict) else []
    if not isinstance(users, list):
        return []
    return [raw for raw in users if isinstance(raw, dict)]


def _load_user_targets(
    users_db_path: Path = USERS_DB_PATH,
    telegram_user_chat_links: Dict[str, Dict[str, str]] | None = None,
) -> List[Dict[str, Any]]:
    users = _load_user_records(users_db_path)
    chat_links = telegram_user_chat_links if isinstance(telegram_user_chat_links, dict) else {}

    targets: List[Dict[str, Any]] = []
    for raw in users:
        user_id = str(raw.get("id", "")).strip()
        link_payload = chat_links.get(user_id, {}) if isinstance(chat_links.get(user_id, {}), dict) else {}
        chat_id_override = _normalizar_telegram_chat_id(link_payload.get("chat_id", ""))
        chat_id_db = _normalizar_telegram_chat_id(raw.get("telegram_chat_id", ""))
        chat_id = chat_id_override or chat_id_db
        if not user_id:
            continue
        if chat_id and not _chat_id_telegram_valido(chat_id):
            chat_id = ""

        targets.append(
            {
                "id": user_id,
                "username": str(raw.get("username", "") or "").strip(),
                "email": str(raw.get("email", "") or "").strip(),
                "chat_id": chat_id,
                "es_premium": _premium_activo_usuario(raw),
            }
        )
    return targets


def _utc_day_key() -> str:
    return datetime.now(pytz.UTC).strftime("%Y-%m-%d")


def _free_user_can_receive_market_alert(state: Dict[str, Any], user_id: str, market_key: str) -> bool:
    free_state = state.setdefault("free_daily_market_alerts", {})
    if not isinstance(free_state, dict):
        state["free_daily_market_alerts"] = {}
        free_state = state["free_daily_market_alerts"]

    user_state = free_state.get(user_id, {})
    if not isinstance(user_state, dict):
        user_state = {}
        free_state[user_id] = user_state

    last_day = str(user_state.get(market_key, "") or "").strip()
    return last_day != _utc_day_key()


def _mark_free_user_market_alert(state: Dict[str, Any], user_id: str, market_key: str) -> None:
    free_state = state.setdefault("free_daily_market_alerts", {})
    if not isinstance(free_state, dict):
        state["free_daily_market_alerts"] = {}
        free_state = state["free_daily_market_alerts"]

    user_state = free_state.get(user_id, {})
    if not isinstance(user_state, dict):
        user_state = {}
        free_state[user_id] = user_state

    user_state[market_key] = _utc_day_key()


def _market_open(kind: str) -> bool:
    if kind == "crypto":
        return True

    now_ny = datetime.now(NY_TZ)
    weekday = now_ny.weekday()
    hhmm = now_ny.hour * 60 + now_ny.minute
    close_min = 17 * 60

    if weekday == 5:  # Saturday
        return False
    if weekday == 6:  # Sunday
        return hhmm >= close_min
    if weekday == 4:  # Friday
        return hhmm < close_min
    return True


def _fetch_twelvedata(symbol: str, interval: str, api_key: str) -> Tuple[pd.DataFrame | None, str]:
    try:
        params = {
            "symbol": symbol,
            "interval": interval,
            "outputsize": 500,
            "format": "JSON",
            "apikey": api_key,
        }
        resp = requests.get("https://api.twelvedata.com/time_series", params=params, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        values = payload.get("values")
        if not values:
            return None, f"TwelveData sin velas para {symbol} {interval}"

        df = pd.DataFrame(values)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime").set_index("datetime")
        for col in ("open", "high", "low", "close", "volume"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.rename(
            columns={
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
            }
        )
        return df, ""
    except Exception as exc:
        return None, str(exc)


def _fetch_data(
    item: MarketItem,
    period: str,
    interval: str,
    cfg: Dict[str, Any] | None = None,
) -> Tuple[pd.DataFrame | None, str, str]:
    cfg_safe = cfg if isinstance(cfg, dict) else {}
    if item.kind == "crypto" and item.binance_symbol:
        binance_df, binance_err = fetch_klines(item.binance_symbol, interval, limit=500)
        if binance_df is not None and not binance_df.empty:
            binance_df = _trim_df_for_interval(binance_df, interval=interval, cfg=cfg_safe)
            return binance_df, "bybit", ""
        logging.warning(
            "[%s] Bybit futures fallo (%s), usando fallback de datos",
            item.state_key,
            binance_err or "sin detalle",
        )

    td_key = os.getenv("TWELVE_DATA_API_KEY", "").strip()
    td_key_upper = td_key.upper()
    td_key_valid = td_key_upper not in {"", "TU_API_KEY", "YOUR_API_KEY", "CHANGE_ME"}
    td_interval = TD_INTERVAL_MAP.get(interval)
    if td_key_valid and td_interval and item.td_symbol:
        td_df, td_err = _fetch_twelvedata(item.td_symbol, td_interval, td_key)
        if td_df is not None and not td_df.empty:
            td_df = _trim_df_for_interval(td_df, interval=interval, cfg=cfg_safe)
            return td_df, "twelvedata", ""
        logging.warning(
            "[%s] TwelveData fallo (%s), usando yfinance fallback",
            item.state_key,
            td_err or "sin detalle",
        )

    try:
        yf_df = obtener_datos(item.ticker, periodo=period, intervalo=interval)
        yf_df = _trim_df_for_interval(yf_df, interval=interval, cfg=cfg_safe)
        return yf_df, "yfinance", ""
    except Exception as exc:
        return None, "", str(exc)


def _now_iso_utc() -> str:
    return datetime.now(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _index_to_iso_utc(value: Any) -> str:
    try:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ""


def _format_price(value: Any) -> str:
    try:
        price = float(value)
        if pd.isna(price):
            return "N/A"
        return f"{price:.10f}".rstrip("0").rstrip(".")
    except Exception:
        return "N/A"


def _parse_iso_utc(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
    except Exception:
        return None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        val = float(value)
        if pd.isna(val):
            return default
        return val
    except Exception:
        return default


def _interval_to_timedelta(interval: str) -> timedelta | None:
    interval = str(interval or "").strip().lower()
    mapping = {
        "1m": timedelta(minutes=1),
        "5m": timedelta(minutes=5),
        "15m": timedelta(minutes=15),
        "30m": timedelta(minutes=30),
        "1h": timedelta(hours=1),
        "4h": timedelta(hours=4),
        "1d": timedelta(days=1),
    }
    return mapping.get(interval)


def _period_for_interval(interval: str) -> str:
    interval = str(interval or "").strip().lower()
    period_map = {
        "1m": "1d",
        "5m": "5d",
        "15m": "5d",
        "30m": "1mo",
        "1h": "3mo",
        "4h": "12mo",
        "1d": "3y",
    }
    return period_map.get(interval, "5d")


def _atr_ratio_from_df(df: pd.DataFrame) -> float:
    if df is None or df.empty or len(df) < 15:
        return 1.0
    try:
        tr = (df["High"] - df["Low"]).abs()
        atr14 = _safe_float(tr.rolling(14).mean().iloc[-1], default=0.0)
        atr50 = _safe_float(tr.rolling(50).mean().iloc[-1], default=0.0) if len(df) >= 50 else _safe_float(tr.mean(), 0.0)
        if atr14 <= 0 or atr50 <= 0:
            return 1.0
        return max(0.01, atr14 / atr50)
    except Exception:
        return 1.0


def _atr14_from_df(df: pd.DataFrame) -> float:
    if df is None or df.empty:
        return 0.0
    try:
        tr = (df["High"] - df["Low"]).abs()
        if len(df) >= 14:
            val = _safe_float(tr.rolling(14).mean().iloc[-1], 0.0)
        else:
            val = _safe_float(tr.mean(), 0.0)
        return max(0.0, val)
    except Exception:
        return 0.0


def _is_last_candle_closed(df: pd.DataFrame, interval: str, grace_seconds: int = 10) -> bool:
    if df is None or df.empty:
        return False
    delta = _interval_to_timedelta(interval)
    if delta is None:
        return True
    try:
        ts = pd.Timestamp(df.index[-1])
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        close_at = ts + delta + timedelta(seconds=max(0, int(grace_seconds)))
        return datetime.now(pytz.UTC) >= close_at.to_pydatetime()
    except Exception:
        return True


def _prepare_df_for_closed_candle(
    df: pd.DataFrame,
    interval: str,
    require_closed_candle: bool,
    grace_seconds: int,
) -> Tuple[pd.DataFrame, bool, bool]:
    if df is None or df.empty:
        return pd.DataFrame(), False, False
    if not require_closed_candle:
        return df, True, False
    is_closed = _is_last_candle_closed(df, interval=interval, grace_seconds=grace_seconds)
    if is_closed:
        return df, True, False
    if len(df) < 3:
        return pd.DataFrame(), False, False
    # Descarta la vela en formación y analiza solo velas cerradas.
    return df.iloc[:-1], True, True


def _candle_metrics(row: pd.Series) -> Dict[str, float]:
    high = _safe_float(row.get("High"))
    low = _safe_float(row.get("Low"))
    open_ = _safe_float(row.get("Open"))
    close = _safe_float(row.get("Close"))
    total_range = max(1e-9, high - low)
    body = abs(close - open_)
    upper_wick = max(0.0, high - max(open_, close))
    lower_wick = max(0.0, min(open_, close) - low)
    return {
        "range": total_range,
        "body": body,
        "upper_wick": upper_wick,
        "lower_wick": lower_wick,
        "open": open_,
        "close": close,
    }


def _detect_price_action(df: pd.DataFrame, direction: str) -> Dict[str, Any]:
    if df is None or df.empty or len(df) < 3:
        return {
            "pattern": "sin_patron",
            "bias": "NEUTRAL",
            "score": 0,
            "aligned": False,
            "description": "Sin suficientes velas para confirmar price action.",
        }
    prev = df.iloc[-2]
    last = df.iloc[-1]
    prev_m = _candle_metrics(prev)
    last_m = _candle_metrics(last)

    prev_bull = prev_m["close"] > prev_m["open"]
    prev_bear = prev_m["close"] < prev_m["open"]
    last_bull = last_m["close"] > last_m["open"]
    last_bear = last_m["close"] < last_m["open"]

    bullish_engulfing = (
        last_bull
        and prev_bear
        and last_m["open"] <= prev_m["close"]
        and last_m["close"] >= prev_m["open"]
    )
    bearish_engulfing = (
        last_bear
        and prev_bull
        and last_m["open"] >= prev_m["close"]
        and last_m["close"] <= prev_m["open"]
    )

    bullish_rejection = (
        last_m["lower_wick"] >= (last_m["body"] * 2.0)
        and (last_m["upper_wick"] <= last_m["body"] * 1.2)
        and last_m["close"] >= (last_m["open"] - 1e-9)
    )
    bearish_rejection = (
        last_m["upper_wick"] >= (last_m["body"] * 2.0)
        and (last_m["lower_wick"] <= last_m["body"] * 1.2)
        and last_m["close"] <= (last_m["open"] + 1e-9)
    )

    pattern = "sin_patron"
    bias = "NEUTRAL"
    description = "Sin patron de confirmacion fuerte."
    score = 2
    if bullish_engulfing:
        pattern = "envolvente_alcista"
        bias = "ALCISTA"
        score = 10
        description = "Vela envolvente alcista detectada."
    elif bearish_engulfing:
        pattern = "envolvente_bajista"
        bias = "BAJISTA"
        score = 10
        description = "Vela envolvente bajista detectada."
    elif bullish_rejection:
        pattern = "rechazo_alcista"
        bias = "ALCISTA"
        score = 8
        description = "Vela de rechazo alcista (mecha inferior dominante)."
    elif bearish_rejection:
        pattern = "rechazo_bajista"
        bias = "BAJISTA"
        score = 8
        description = "Vela de rechazo bajista (mecha superior dominante)."

    direction_norm = str(direction or "").upper().strip()
    aligned = bias == direction_norm and bias in {"ALCISTA", "BAJISTA"}
    if bias in {"ALCISTA", "BAJISTA"} and not aligned:
        score = 0

    return {
        "pattern": pattern,
        "bias": bias,
        "score": score,
        "aligned": aligned,
        "description": description,
    }


def _opposite_direction(direction: str) -> str:
    direction = str(direction or "").upper().strip()
    if direction == "ALCISTA":
        return "BAJISTA"
    if direction == "BAJISTA":
        return "ALCISTA"
    return "NEUTRAL"


def _resolve_precision_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    raw = cfg.get("precision_filters", {})
    if isinstance(raw, dict):
        return raw
    return {}


def _normalize_alert_profile(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"conservador", "conservative"}:
        return "conservador"
    if raw in {"agresivo", "aggressive"}:
        return "agresivo"
    return "balanceado"


def _apply_profile_to_precision_cfg(precision_cfg: Dict[str, Any]) -> Dict[str, Any]:
    cfg = dict(precision_cfg)
    profile = _normalize_alert_profile(cfg.get("alert_profile", "balanceado"))
    cfg["alert_profile"] = profile

    base_conf = int(cfg.get("min_confidence_score", 85))
    base_rr = _safe_float(cfg.get("min_rr", 1.8), 1.8)
    base_mtf = max(1, int(cfg.get("min_mtf_confirmations", 2)))
    base_persistence = max(1, int(cfg.get("persistence_bars", 2)))

    if profile == "conservador":
        cfg["min_confidence_score"] = max(base_conf, 85)
        cfg["min_rr"] = max(base_rr, 1.8)
        cfg["min_mtf_confirmations"] = max(base_mtf, 2)
        cfg["require_price_action_confirmation"] = True
        cfg["persistence_bars"] = max(base_persistence, 2)
    elif profile == "agresivo":
        cfg["min_confidence_score"] = min(base_conf, 78)
        cfg["min_rr"] = min(base_rr, 1.5)
        cfg["min_mtf_confirmations"] = 1
        cfg["require_price_action_confirmation"] = False
        cfg["persistence_bars"] = 1
    else:
        cfg["min_confidence_score"] = base_conf
        cfg["min_rr"] = base_rr
        cfg["min_mtf_confirmations"] = base_mtf
        cfg["persistence_bars"] = base_persistence

    return cfg


def _quality_metrics_from_record(record: Dict[str, Any]) -> Dict[str, Any]:
    stats = record.get("quality_stats", {})
    if not isinstance(stats, dict):
        stats = {}
    wins = int(stats.get("wins", 0) or 0)
    losses = int(stats.get("losses", 0) or 0)
    timeouts = int(stats.get("timeouts", 0) or 0)
    resolved = int(stats.get("resolved", wins + losses + timeouts) or (wins + losses + timeouts))
    if resolved < 0:
        resolved = 0

    accuracy_pct = (wins / resolved * 100.0) if resolved > 0 else 0.0
    noise_pct = ((losses + timeouts) / resolved * 100.0) if resolved > 0 else 0.0
    timeout_pct = (timeouts / resolved * 100.0) if resolved > 0 else 0.0

    rr_values: List[float] = []
    history = record.get("quality_history", [])
    if isinstance(history, list):
        for event in history:
            if not isinstance(event, dict):
                continue
            status = str(event.get("status", "")).strip().lower()
            if status not in {"win", "loss", "timeout"}:
                continue
            rr = _safe_float(event.get("rr_estimado"), 0.0)
            if rr > 0:
                rr_values.append(rr)

    rr_avg = _safe_float(stats.get("rr_avg"), 0.0)
    rr_samples = int(stats.get("rr_samples", 0) or 0)
    if rr_values:
        rr_samples = len(rr_values)
        rr_avg = sum(rr_values) / rr_samples
    elif rr_samples <= 0:
        rr_avg = 0.0
        rr_samples = 0

    return {
        "wins": wins,
        "losses": losses,
        "timeouts": timeouts,
        "resolved": resolved,
        "accuracy_pct": round(accuracy_pct, 2),
        "noise_pct": round(noise_pct, 2),
        "timeout_pct": round(timeout_pct, 2),
        "rr_avg": round(rr_avg, 3),
        "rr_samples": rr_samples,
    }


def _aggregate_quality_stats(state: Dict[str, Any]) -> Dict[str, Any]:
    symbols = state.get("symbols", {})
    if not isinstance(symbols, dict):
        return {
            "resolved": 0,
            "wins": 0,
            "losses": 0,
            "timeouts": 0,
            "accuracy_pct": 0.0,
            "noise_pct": 0.0,
            "timeout_pct": 0.0,
            "rr_avg": 0.0,
        }

    wins = 0
    losses = 0
    timeouts = 0
    rr_weighted_sum = 0.0
    rr_weighted_count = 0
    for record in symbols.values():
        if not isinstance(record, dict):
            continue
        metrics = _quality_metrics_from_record(record)
        wins += int(metrics.get("wins", 0) or 0)
        losses += int(metrics.get("losses", 0) or 0)
        timeouts += int(metrics.get("timeouts", 0) or 0)
        rr_samples = int(metrics.get("rr_samples", 0) or 0)
        rr_avg = _safe_float(metrics.get("rr_avg"), 0.0)
        if rr_samples > 0 and rr_avg > 0:
            rr_weighted_sum += rr_avg * rr_samples
            rr_weighted_count += rr_samples

    resolved = wins + losses + timeouts
    accuracy_pct = (wins / resolved * 100.0) if resolved > 0 else 0.0
    noise_pct = ((losses + timeouts) / resolved * 100.0) if resolved > 0 else 0.0
    timeout_pct = (timeouts / resolved * 100.0) if resolved > 0 else 0.0
    rr_avg = (rr_weighted_sum / rr_weighted_count) if rr_weighted_count > 0 else 0.0
    return {
        "resolved": resolved,
        "wins": wins,
        "losses": losses,
        "timeouts": timeouts,
        "accuracy_pct": round(accuracy_pct, 2),
        "noise_pct": round(noise_pct, 2),
        "timeout_pct": round(timeout_pct, 2),
        "rr_avg": round(rr_avg, 3),
    }


def _apply_quality_calibration(precision_cfg: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    cfg = dict(precision_cfg)
    calibration_enabled = bool(cfg.get("quality_calibration_enabled", True))
    min_resolved = max(5, int(cfg.get("quality_calibration_min_resolved", 20)))
    quality = _aggregate_quality_stats(state)

    conf_delta = 0
    rr_delta = 0.0
    mtf_delta = 0
    mode = "disabled"

    if not calibration_enabled:
        mode = "disabled"
    elif quality["resolved"] < min_resolved:
        mode = "warmup"
    else:
        accuracy_pct = _safe_float(quality.get("accuracy_pct"), 0.0)
        noise_pct = _safe_float(quality.get("noise_pct"), 0.0)
        rr_avg = _safe_float(quality.get("rr_avg"), 0.0)
        if noise_pct >= 55.0 or accuracy_pct < 45.0:
            conf_delta = 4
            rr_delta = 0.2
            mtf_delta = 1
            mode = "tighten_hard"
        elif noise_pct >= 45.0 or accuracy_pct < 55.0 or (rr_avg > 0 and rr_avg < 1.4):
            conf_delta = 2
            rr_delta = 0.1
            mode = "tighten_soft"
        elif accuracy_pct >= 72.0 and noise_pct <= 25.0 and rr_avg >= 1.9:
            conf_delta = -1
            rr_delta = -0.05
            mode = "relax_soft"
        else:
            mode = "neutral"

    base_conf = int(cfg.get("min_confidence_score", 85))
    base_rr = _safe_float(cfg.get("min_rr", 1.8), 1.8)
    base_mtf = max(1, int(cfg.get("min_mtf_confirmations", 1)))

    cfg["min_confidence_score"] = max(40, min(99, base_conf + conf_delta))
    cfg["min_rr"] = round(max(0.8, base_rr + rr_delta), 2)
    cfg["min_mtf_confirmations"] = max(1, base_mtf + mtf_delta)
    cfg["quality_calibration"] = {
        "scope": "global",
        "mode": mode,
        "resolved": int(quality.get("resolved", 0) or 0),
        "accuracy_pct": _safe_float(quality.get("accuracy_pct"), 0.0),
        "noise_pct": _safe_float(quality.get("noise_pct"), 0.0),
        "timeout_pct": _safe_float(quality.get("timeout_pct"), 0.0),
        "rr_avg": _safe_float(quality.get("rr_avg"), 0.0),
        "min_resolved_required": min_resolved,
        "deltas": {
            "conf": conf_delta,
            "rr": round(rr_delta, 2),
            "mtf": mtf_delta,
        },
    }
    return cfg


def _resolve_effective_precision_cfg(cfg: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    base = _resolve_precision_cfg(cfg)
    profile_cfg = _apply_profile_to_precision_cfg(base)
    return _apply_quality_calibration(profile_cfg, state)


def _apply_record_quality_calibration(
    precision_cfg: Dict[str, Any],
    record: Dict[str, Any],
    record_key: str,
) -> Dict[str, Any]:
    cfg = dict(precision_cfg)
    if not bool(cfg.get("quality_calibration_enabled", True)):
        return cfg
    scope = str(cfg.get("quality_calibration_scope", "global_and_record")).strip().lower()
    if scope not in {"global_and_record", "record"}:
        return cfg
    if not bool(cfg.get("quality_calibration_record_enabled", True)):
        return cfg

    min_resolved = max(4, int(cfg.get("quality_calibration_record_min_resolved", 8)))
    metrics = _quality_metrics_from_record(record)

    conf_delta = 0
    rr_delta = 0.0
    mtf_delta = 0
    mode = "disabled"

    if metrics["resolved"] < min_resolved:
        mode = "warmup"
    else:
        accuracy_pct = _safe_float(metrics.get("accuracy_pct"), 0.0)
        noise_pct = _safe_float(metrics.get("noise_pct"), 0.0)
        rr_avg = _safe_float(metrics.get("rr_avg"), 0.0)
        if noise_pct >= 58.0 or accuracy_pct < 44.0 or (rr_avg > 0 and rr_avg < 1.2):
            conf_delta = 3
            rr_delta = 0.15
            mtf_delta = 1
            mode = "tighten_hard"
        elif noise_pct >= 48.0 or accuracy_pct < 54.0 or (rr_avg > 0 and rr_avg < 1.4):
            conf_delta = 1
            rr_delta = 0.05
            mode = "tighten_soft"
        elif accuracy_pct >= 70.0 and noise_pct <= 26.0 and rr_avg >= 1.9:
            conf_delta = -1
            mode = "relax_soft"
        else:
            mode = "neutral"

    base_conf = int(cfg.get("min_confidence_score", 85))
    base_rr = _safe_float(cfg.get("min_rr", 1.8), 1.8)
    base_mtf = max(1, int(cfg.get("min_mtf_confirmations", 1)))
    cfg["min_confidence_score"] = max(40, min(99, base_conf + conf_delta))
    cfg["min_rr"] = round(max(0.8, base_rr + rr_delta), 2)
    cfg["min_mtf_confirmations"] = max(1, base_mtf + mtf_delta)

    global_cal = cfg.get("quality_calibration", {})
    if not isinstance(global_cal, dict):
        global_cal = {}
    cfg["quality_calibration"] = {
        "scope": "global_and_record" if scope == "global_and_record" else "record",
        "mode": mode,
        "record_key": record_key,
        "resolved": int(metrics.get("resolved", 0) or 0),
        "accuracy_pct": _safe_float(metrics.get("accuracy_pct"), 0.0),
        "noise_pct": _safe_float(metrics.get("noise_pct"), 0.0),
        "timeout_pct": _safe_float(metrics.get("timeout_pct"), 0.0),
        "rr_avg": _safe_float(metrics.get("rr_avg"), 0.0),
        "min_resolved_required": min_resolved,
        "global_mode": str(global_cal.get("mode", "n/a")),
        "global_accuracy_pct": _safe_float(global_cal.get("accuracy_pct"), 0.0),
        "global_noise_pct": _safe_float(global_cal.get("noise_pct"), 0.0),
        "deltas": {
            "conf": conf_delta,
            "rr": round(rr_delta, 2),
            "mtf": mtf_delta,
        },
    }
    cfg["quality_metrics"] = metrics
    return cfg


def _resolve_mtf_intervals(base_interval: str, precision_cfg: Dict[str, Any]) -> List[str]:
    mapping = precision_cfg.get("mtf_intervals", {})
    if isinstance(mapping, dict):
        raw = mapping.get(base_interval, [])
        if isinstance(raw, list):
            values = [str(x).strip().lower() for x in raw if str(x).strip()]
            if values:
                return values
    if base_interval in {"15m", "30m"}:
        return ["1h", "4h", "1d"]
    if base_interval == "1h":
        return ["4h", "1d"]
    return []


def _compute_interval_direction(
    item: MarketItem,
    interval: str,
    cache: Dict[str, Dict[str, Any]],
    require_closed_candle: bool = True,
    grace_seconds: int = 10,
    cfg: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    cache_key = f"{item.state_key}|{interval}"
    if cache_key in cache:
        return cache[cache_key]

    period = _period_for_interval(interval)
    df, source, err = _fetch_data(item, period=period, interval=interval, cfg=cfg)
    if df is None or df.empty:
        payload = {
            "ok": False,
            "interval": interval,
            "source": source,
            "error": err or "sin datos",
            "direction": "NEUTRAL",
        }
        cache[cache_key] = payload
        return payload

    df_ready, ready_ok, _ = _prepare_df_for_closed_candle(
        df,
        interval=interval,
        require_closed_candle=require_closed_candle,
        grace_seconds=grace_seconds,
    )
    if not ready_ok or df_ready.empty:
        payload = {
            "ok": False,
            "interval": interval,
            "source": source,
            "error": "sin velas cerradas",
            "direction": "NEUTRAL",
        }
        cache[cache_key] = payload
        return payload

    try:
        df_ind = calcular_indicadores(df_ready)
        estado = construir_estado_final(df_ind, impacto_memoria=0)
        direction = str(estado.get("direccion_v13", "NEUTRAL")).upper().strip() or "NEUTRAL"
    except Exception as exc:
        payload = {
            "ok": False,
            "interval": interval,
            "source": source,
            "error": str(exc),
            "direction": "NEUTRAL",
        }
        cache[cache_key] = payload
        return payload

    payload = {
        "ok": True,
        "interval": interval,
        "source": source,
        "error": "",
        "direction": direction,
    }
    cache[cache_key] = payload
    return payload


def _evaluate_mtf_alignment(
    item: MarketItem,
    base_interval: str,
    base_direction: str,
    precision_cfg: Dict[str, Any],
    cache: Dict[str, Dict[str, Any]],
    cfg: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    base_direction = str(base_direction or "").upper().strip()
    if base_direction not in {"ALCISTA", "BAJISTA"}:
        return {
            "ok": False,
            "score": 0,
            "details": [],
            "summary": "Direccion base neutral, sin alineacion MTF.",
            "confirmations": 0,
            "opposites": 0,
        }

    intervals = _resolve_mtf_intervals(base_interval, precision_cfg)
    if not intervals:
        return {
            "ok": True,
            "score": 12,
            "details": [],
            "summary": "Sin filtros MTF requeridos para este timeframe.",
            "confirmations": 0,
            "opposites": 0,
            "neutrals": 0,
        }

    opposite = _opposite_direction(base_direction)
    confirmations = 0
    opposites = 0
    neutrals = 0
    details: List[str] = []
    mandatory_map = {
        "15m": ["1h", "4h"],
        "30m": ["1h", "4h"],
        "1h": ["4h", "1d"],
    }
    mandatory = [x for x in mandatory_map.get(base_interval, []) if x in intervals]
    mandatory_ok = True
    require_closed_candle = bool(precision_cfg.get("require_closed_candle", True))
    grace_seconds = int(precision_cfg.get("closed_candle_grace_sec", 10))

    for tf in intervals:
        res = _compute_interval_direction(
            item=item,
            interval=tf,
            cache=cache,
            require_closed_candle=require_closed_candle,
            grace_seconds=grace_seconds,
            cfg=cfg,
        )
        if not res.get("ok", False):
            details.append(f"{tf}: error({res.get('error', 'sin detalle')})")
            if tf in mandatory:
                mandatory_ok = False
            continue
        direction = str(res.get("direction", "NEUTRAL")).upper().strip()
        details.append(f"{tf}:{direction}")
        if direction == base_direction:
            confirmations += 1
        elif direction == opposite:
            opposites += 1
            if tf in mandatory:
                mandatory_ok = False
        else:
            neutrals += 1
            if tf in mandatory:
                mandatory_ok = False

    min_confirms = max(1, int(precision_cfg.get("min_mtf_confirmations", 2)))
    ok = mandatory_ok and opposites == 0 and confirmations >= min_confirms
    score = 0
    if ok:
        score = 20 if neutrals == 0 else 16
    elif opposites == 0 and confirmations >= 1:
        score = 10

    return {
        "ok": ok,
        "score": score,
        "details": details,
        "summary": f"confirmaciones={confirmations}, opuestos={opposites}, neutrales={neutrals}",
        "confirmations": confirmations,
        "opposites": opposites,
        "neutrals": neutrals,
    }


def _compute_dynamic_min_confidence(
    base_interval: str,
    vol_ratio: float,
    precision_cfg: Dict[str, Any],
) -> int:
    base = int(precision_cfg.get("min_confidence_score", 85))
    if not bool(precision_cfg.get("adaptive_threshold", True)):
        return max(40, min(99, base))

    adjust = 0
    if vol_ratio >= 1.5:
        adjust += 5
    elif vol_ratio >= 1.3:
        adjust += 3
    elif vol_ratio <= 0.8:
        adjust -= 2

    tf_adjust = {"5m": 4, "15m": 3, "30m": 2, "1h": 1, "4h": 0, "1d": 0}
    adjust += tf_adjust.get(base_interval, 0)
    return max(40, min(99, base + adjust))


def _compute_adaptive_cooldown_minutes(
    cfg: Dict[str, Any],
    base_interval: str,
    vol_ratio: float,
    precision_cfg: Dict[str, Any],
) -> int:
    fallback = max(1, int(cfg.get("cooldown_minutes", 60)))
    base_map = precision_cfg.get("base_cooldown_by_interval", {})
    if isinstance(base_map, dict):
        base = int(base_map.get(base_interval, fallback))
    else:
        base = fallback

    if not bool(precision_cfg.get("adaptive_cooldown", True)):
        return max(1, base)

    factor = 1.0
    if vol_ratio >= 1.8:
        factor = 1.8
    elif vol_ratio >= 1.5:
        factor = 1.5
    elif vol_ratio >= 1.3:
        factor = 1.3
    elif vol_ratio <= 0.75:
        factor = 0.8
    elif vol_ratio <= 0.9:
        factor = 0.9

    return max(1, int(round(base * factor)))


def _compute_signal_confidence(
    estado: Dict[str, Any],
    mtf_info: Dict[str, Any],
    candle_info: Dict[str, Any],
) -> int:
    dorado = estado.get("dorado_v13") or {}
    micro_score = _safe_float(dorado.get("micro_score"), 0.0)
    umbral = _safe_float(dorado.get("umbral"), 1.0)
    rr = _safe_float(dorado.get("rr_estimado"), 0.0)

    score_component = min(1.0, micro_score / max(umbral + 2.0, 1.0)) * 45.0
    rr_component = min(1.0, rr / 2.5) * 25.0
    mtf_component = _safe_float(mtf_info.get("score"), 0.0)
    candle_component = _safe_float(candle_info.get("score"), 0.0)

    raw = score_component + rr_component + mtf_component + candle_component
    return max(0, min(100, int(round(raw))))


def _should_allow_pullback_neutral_mtf(
    *,
    setup_tipo: str,
    base_interval: str,
    rr: float,
    mtf_info: Dict[str, Any],
    confidence_score: int,
    min_confidence: int,
) -> bool:
    if str(setup_tipo or "").strip().lower() != "pullback_tendencia":
        return False
    if str(base_interval or "").strip() not in {"15m", "30m"}:
        return False
    if _safe_float(rr, 0.0) < 2.0:
        return False
    if int(confidence_score or 0) < max(0, int(min_confidence or 0) - 6):
        return False
    if int(mtf_info.get("opposites", 0) or 0) != 0:
        return False
    if int(mtf_info.get("confirmations", 0) or 0) != 0:
        return False
    if int(mtf_info.get("neutrals", 0) or 0) <= 0:
        return False
    return True


def _compute_dorado_active_state(
    signal_ready: bool,
    current_streak: int,
    persistence_bars: int,
) -> bool:
    if not signal_ready:
        return False
    return int(current_streak or 0) >= max(1, int(persistence_bars))


def _daily_alert_count(record: Dict[str, Any]) -> int:
    day_key = _utc_day_key()
    per_day = record.get("daily_alert_counts", {})
    if not isinstance(per_day, dict):
        return 0
    try:
        return int(per_day.get(day_key, 0) or 0)
    except Exception:
        return 0


def _increment_daily_alert_count(record: Dict[str, Any]) -> None:
    day_key = _utc_day_key()
    per_day = record.get("daily_alert_counts", {})
    if not isinstance(per_day, dict):
        per_day = {}
    # Limpieza simple: conservar solo hoy y ayer para no crecer infinito.
    keep: Dict[str, Any] = {}
    for k, v in per_day.items():
        if k == day_key:
            keep[k] = v
            continue
        try:
            dt = datetime.strptime(k, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
            if (datetime.now(pytz.UTC) - dt).days <= 1:
                keep[k] = v
        except Exception:
            continue
    keep[day_key] = int(keep.get(day_key, 0) or 0) + 1
    record["daily_alert_counts"] = keep


def _quality_window_bars(interval: str, precision_cfg: Dict[str, Any]) -> int:
    default_bars = max(1, int(precision_cfg.get("quality_window_bars", 12)))
    raw = precision_cfg.get("quality_window_bars_by_interval", {})
    if isinstance(raw, dict):
        try:
            return max(1, int(raw.get(interval, default_bars)))
        except Exception:
            return default_bars
    return default_bars


def _append_quality_event(record: Dict[str, Any], event: Dict[str, Any]) -> None:
    history = record.get("quality_history", [])
    if not isinstance(history, list):
        history = []
    history.append(event)
    record["quality_history"] = history[-120:]


def _update_quality_stats(record: Dict[str, Any], outcome: str) -> None:
    stats = record.get("quality_stats", {})
    if not isinstance(stats, dict):
        stats = {}
    total = int(stats.get("total", 0) or 0) + 1
    wins = int(stats.get("wins", 0) or 0)
    losses = int(stats.get("losses", 0) or 0)
    timeouts = int(stats.get("timeouts", 0) or 0)
    replaced = int(stats.get("replaced", 0) or 0)

    if outcome == "win":
        wins += 1
    elif outcome == "loss":
        losses += 1
    elif outcome == "timeout":
        timeouts += 1
    elif outcome == "replaced":
        replaced += 1

    resolved = wins + losses + timeouts
    accuracy = (wins / resolved * 100.0) if resolved > 0 else 0.0
    timeout_pct = (timeouts / resolved * 100.0) if resolved > 0 else 0.0
    noise_pct = ((losses + timeouts) / resolved * 100.0) if resolved > 0 else 0.0
    stats.update(
        {
            "total": total,
            "wins": wins,
            "losses": losses,
            "timeouts": timeouts,
            "replaced": replaced,
            "resolved": resolved,
            "accuracy_pct": round(accuracy, 2),
            "timeout_pct": round(timeout_pct, 2),
            "noise_pct": round(noise_pct, 2),
            "updated_utc": _now_iso_utc(),
        }
    )
    record["quality_stats"] = stats
    metrics = _quality_metrics_from_record(record)
    stats["rr_avg"] = _safe_float(metrics.get("rr_avg"), 0.0)
    stats["rr_samples"] = int(metrics.get("rr_samples", 0) or 0)
    stats["timeout_pct"] = _safe_float(metrics.get("timeout_pct"), 0.0)
    stats["noise_pct"] = _safe_float(metrics.get("noise_pct"), 0.0)
    record["quality_stats"] = stats


def _open_alert_snapshot(
    estado: Dict[str, Any],
    precision: Dict[str, Any],
    compute_ctx: Dict[str, Any],
    precision_cfg: Dict[str, Any],
) -> Dict[str, Any] | None:
    direction = str(estado.get("direccion_v13", "")).upper().strip()
    entry_price = _safe_float(estado.get("precio_alerta"), 0.0)
    interval = str(precision.get("interval", compute_ctx.get("interval", ""))).strip() or "15m"
    if direction not in {"ALCISTA", "BAJISTA"} or entry_price <= 0:
        return None

    operational_plan = precision.get("operational_plan", {}) if isinstance(precision.get("operational_plan", {}), dict) else {}
    if not bool(operational_plan.get("ok", False)):
        return None

    sl_price = _safe_float(operational_plan.get("sl_price"), 0.0)
    tp_price = _safe_float(operational_plan.get("tp_price"), 0.0)
    risk_1r = abs(entry_price - sl_price)
    if risk_1r <= 0 or tp_price <= 0:
        return None

    return {
        "status": "open",
        "opened_utc": _now_iso_utc(),
        "opened_bar_utc": str(estado.get("indice_alerta_utc", "")).strip(),
        "last_bar_utc": str(estado.get("indice_alerta_utc", "")).strip(),
        "bars_elapsed": 0,
        "max_bars": _quality_window_bars(interval, precision_cfg),
        "interval": interval,
        "direction": direction,
        "entry_price": round(entry_price, 10),
        "risk_1r": round(risk_1r, 10),
        "tp_price": round(tp_price, 10),
        "sl_price": round(sl_price, 10),
        "confidence_score": int(precision.get("confidence_score", 0) or 0),
        "rr_estimado": _safe_float((estado.get("dorado_v13") or {}).get("rr_estimado"), _safe_float(operational_plan.get("rr_ratio"), OPERATIONAL_TP_R_MULT)),
        "risk_pct_operativo": _safe_float(operational_plan.get("risk_pct"), 0.0),
        "risk_pct_structural": _safe_float(operational_plan.get("risk_pct_structural"), 0.0),
        "reasons": list(precision.get("reasons", [])),
    }


def _evaluate_open_alert_outcome(
    record: Dict[str, Any],
    estado: Dict[str, Any],
    compute_ctx: Dict[str, Any],
) -> None:
    open_alert = record.get("open_alert")
    if not isinstance(open_alert, dict):
        return
    if str(open_alert.get("status", "")).strip().lower() != "open":
        return

    df_ind = compute_ctx.get("df_ind")
    if not isinstance(df_ind, pd.DataFrame) or df_ind.empty:
        return

    last_row = df_ind.iloc[-1]
    high = _safe_float(last_row.get("High"), 0.0)
    low = _safe_float(last_row.get("Low"), 0.0)
    close = _safe_float(last_row.get("Close"), 0.0)
    bar_utc = str(estado.get("indice_alerta_utc", "")).strip()
    last_bar = str(open_alert.get("last_bar_utc", "")).strip()
    bars_elapsed = int(open_alert.get("bars_elapsed", 0) or 0)

    if bar_utc and bar_utc != last_bar:
        bars_elapsed += 1
        open_alert["last_bar_utc"] = bar_utc
    open_alert["bars_elapsed"] = bars_elapsed
    open_alert["last_price"] = round(close, 10)

    direction = str(open_alert.get("direction", "")).upper().strip()
    tp_price = _safe_float(open_alert.get("tp_price"), 0.0)
    sl_price = _safe_float(open_alert.get("sl_price"), 0.0)
    max_bars = max(1, int(open_alert.get("max_bars", 12) or 12))

    win_hit = False
    loss_hit = False
    if direction == "ALCISTA":
        win_hit = high >= tp_price > 0
        loss_hit = low <= sl_price < tp_price
    elif direction == "BAJISTA":
        win_hit = low <= tp_price > 0
        loss_hit = high >= sl_price > tp_price

    outcome = ""
    note = ""
    if win_hit and loss_hit:
        # Conservador: si en la misma vela toca TP y SL, lo marcamos como loss.
        outcome = "loss"
        note = "tp_y_sl_misma_vela"
    elif win_hit:
        outcome = "win"
        note = "tp_alcanzado"
    elif loss_hit:
        outcome = "loss"
        note = "sl_alcanzado"
    elif bars_elapsed >= max_bars:
        outcome = "timeout"
        note = "ventana_expirada"

    if not outcome:
        record["open_alert"] = open_alert
        return

    open_alert["status"] = outcome
    open_alert["closed_utc"] = _now_iso_utc()
    open_alert["bars_elapsed"] = bars_elapsed
    open_alert["close_price"] = round(close, 10)
    open_alert["outcome_note"] = note

    _append_quality_event(record, dict(open_alert))
    _update_quality_stats(record, outcome)
    record["last_quality_outcome"] = outcome
    record["open_alert"] = None


def _should_alert(
    record: Dict[str, Any],
    signal_ready: bool,
    cooldown_minutes: int,
    persistence_bars: int,
    max_alerts_per_symbol_day: int,
) -> bool:
    if not signal_ready:
        return False

    streak = int(record.get("dorado_streak", 0) or 0)
    if streak < max(1, int(persistence_bars)):
        return False

    was_dorado = bool(record.get("dorado_active", False))
    if was_dorado:
        return False

    if max_alerts_per_symbol_day > 0 and _daily_alert_count(record) >= max_alerts_per_symbol_day:
        return False

    last_alert_iso = str(record.get("last_alert_utc", "")).strip()
    last_alert_dt = _parse_iso_utc(last_alert_iso) if last_alert_iso else None
    if last_alert_dt is None:
        return True

    delta_sec = (datetime.now(pytz.UTC) - last_alert_dt).total_seconds()
    return delta_sec >= max(1, cooldown_minutes) * 60


def _build_watchlist(cfg: Dict[str, Any]) -> List[MarketItem]:
    watchlist: List[MarketItem] = []

    if bool(cfg.get("scan_crypto", True)):
        selected_crypto = cfg.get("crypto_symbols", list(CRYPTO_MAP.keys()))
        for symbol in selected_crypto:
            row = CRYPTO_MAP.get(symbol)
            if not row:
                logging.warning("Cripto desconocida en config: %s", symbol)
                continue
            watchlist.append(
                MarketItem(
                    market="Cripto",
                    label=symbol,
                    ticker=row["ticker"],
                    td_symbol=row.get("td", ""),
                    kind="crypto",
                    binance_symbol=row.get("binance", ""),
                )
            )

    if bool(cfg.get("scan_gold", True)):
        selected_gold = cfg.get("gold_symbols", list(GOLD_MAP.keys()))
        for symbol in selected_gold:
            row = GOLD_MAP.get(symbol)
            if not row:
                logging.warning("Instrumento de oro desconocido en config: %s", symbol)
                continue
            watchlist.append(
                MarketItem(
                    market="Oro",
                    label=symbol,
                    ticker=row["ticker"],
                    td_symbol=row.get("td", ""),
                    kind="gold",
                )
            )

    return watchlist


def _confirmation_hint(direction: str, pattern: str) -> str:
    direction = str(direction or "").upper().strip()
    pattern = str(pattern or "").strip().lower()

    if direction == "ALCISTA":
        if pattern in {"envolvente_alcista", "rechazo_alcista"}:
            return "continuidad alcista o retesteo con rechazo alcista."
        return "vela alcista de continuidad o retesteo con rechazo."
    if direction == "BAJISTA":
        if pattern in {"envolvente_bajista", "rechazo_bajista"}:
            return "continuidad bajista o retesteo con rechazo bajista."
        return "vela bajista de continuidad o retesteo con rechazo."
    return "confirmacion clara de direccion antes de operar."


def _mentor_line(direction: str, riesgo: str, confidence: Any) -> str:
    direction = str(direction or "").upper().strip()
    riesgo_norm = str(riesgo or "").strip().lower()
    conf = int(_safe_float(confidence, 0))

    if riesgo_norm in {"alto", "muy alto"}:
        return "Mentor: Riesgo elevado, prioriza proteger capital y reduce tamaño."
    if conf and conf < 85:
        return "Mentor: Señal valida, pero sin prisa; opera solo con confirmacion limpia."
    if direction == "ALCISTA":
        return "Mentor: No persigas precio; si no confirma alcista, no hay operacion."
    if direction == "BAJISTA":
        return "Mentor: No persigas precio; si no confirma bajista, no hay operacion."
    return "Mentor: Contexto mixto; espera mejor estructura."


def _alert_action_label(direction: str, strength: str) -> str:
    direction = str(direction or "").upper().strip()
    strength = str(strength or "").upper().strip()

    if direction == "ALCISTA":
        if strength == "FUERTE":
            return "Preparar compra"
        return "Esperar confirmacion de compra"
    if direction == "BAJISTA":
        if strength == "FUERTE":
            return "Preparar venta"
        return "Esperar confirmacion de venta"
    return "Esperar confirmacion"


def _human_direction_label(direction: str) -> str:
    direction = str(direction or "").upper().strip()
    if direction == "ALCISTA":
        return "Alcista"
    if direction == "BAJISTA":
        return "Bajista"
    return "No claro"


def _structural_context_label_from_state(estado: Dict[str, Any]) -> str:
    estructura = estado.get("estructura_1d_4h", {}) if isinstance(estado.get("estructura_1d_4h", {}), dict) else {}
    macro_direction = str(estructura.get("direccion_1d") or estado.get("direccion_v13", "")).upper().strip()
    alignment = str(estructura.get("alineacion", "")).upper().strip()

    if alignment == "CONFLICTO":
        return "En transicion"
    if macro_direction in {"ALCISTA", "BAJISTA"}:
        return _human_direction_label(macro_direction)
    return "No claro"


def _resolve_structural_context_label(
    item: MarketItem,
    estado: Dict[str, Any],
    cfg: Dict[str, Any],
    cache: Dict[str, str] | None = None,
) -> str:
    explicit = str(estado.get("contexto_estructural", "")).strip()
    if explicit:
        return explicit

    local_label = _structural_context_label_from_state(estado)
    if local_label not in {"No claro"}:
        return local_label

    cache_ref = cache if isinstance(cache, dict) else {}
    cache_key = str(item.state_key or item.ticker or item.label).strip()
    if cache_key and cache_key in cache_ref:
        return str(cache_ref.get(cache_key, "")).strip() or "No claro"

    label = ""
    precision_cfg = _resolve_precision_cfg(cfg)
    require_closed_candle = bool(precision_cfg.get("require_closed_candle", True))
    grace_seconds = int(precision_cfg.get("closed_candle_grace_sec", 10))
    try:
        df_1d, _, err_1d = _fetch_data(item, period="3y", interval="1d", cfg=cfg)
        df_4h, _, err_4h = _fetch_data(item, period="12mo", interval="4h", cfg=cfg)
        if (
            err_1d
            or err_4h
            or df_1d is None
            or df_4h is None
            or df_1d.empty
            or df_4h.empty
        ):
            raise ValueError("structural_context_fetch_failed")

        df_1d_ready, ok_1d, _ = _prepare_df_for_closed_candle(
            df_1d,
            interval="1d",
            require_closed_candle=require_closed_candle,
            grace_seconds=grace_seconds,
        )
        df_4h_ready, ok_4h, _ = _prepare_df_for_closed_candle(
            df_4h,
            interval="4h",
            require_closed_candle=require_closed_candle,
            grace_seconds=grace_seconds,
        )
        if not ok_1d or not ok_4h or df_1d_ready.empty or df_4h_ready.empty:
            raise ValueError("structural_context_no_closed_candles")

        structural_state = construir_estado_final_estructural(
            calcular_indicadores(df_1d_ready),
            calcular_indicadores(df_4h_ready),
            impacto_memoria=0,
        )
        label = _structural_context_label_from_state(structural_state)
    except Exception:
        fallback_direction = str(estado.get("direccion_v13", "")).upper().strip()
        if fallback_direction in {"ALCISTA", "BAJISTA"}:
            label = _human_direction_label(fallback_direction)
        else:
            label = "No claro"

    if cache_key:
        cache_ref[cache_key] = label
    return label


def _alert_operational_scenario_label(
    direction: str,
    setup_tipo: str,
    strength: str,
    temporalidad: str,
    modo: str,
) -> str:
    direction = str(direction or "").upper().strip()
    setup_tipo = str(setup_tipo or "").strip().lower()
    strength = str(strength or "").upper().strip()
    temporalidad = str(temporalidad or "").strip().lower()
    modo = str(modo or "").strip().lower()
    direction_text = str(direction or "").lower().strip()
    structural_mode = temporalidad == "1d + 4h" or "estructural" in modo or "1d+4h" in modo

    if direction not in {"ALCISTA", "BAJISTA"}:
        return "Cambio estructural en desarrollo" if structural_mode else "Cambio local en desarrollo"

    if structural_mode and setup_tipo == "pullback_tendencia":
        return f"Pullback estructural {direction_text}"
    if setup_tipo == "pullback_tendencia":
        return f"Pullback {direction_text} de continuidad"
    if strength == "FUERTE":
        return f"Continuacion {direction_text} confirmada"
    return f"Continuacion {direction_text} en desarrollo"


def _mentor_block(direction: str, strength: str) -> str:
    direction = str(direction or "").upper().strip()
    strength = str(strength or "").upper().strip()

    if direction == "ALCISTA":
        if strength == "FUERTE":
            lines = [
                "Mentor:",
                "Lectura: la estructura mantiene presion alcista y la ventaja aparece si el mercado confirma continuacion en zona.",
                "Confirmacion: retesteo defendido o nueva vela de impulso con rechazo vendedor debil.",
                "Invalidacion: si rompe el ultimo piso operativo, la continuacion pierde valor.",
                "Error comun: entrar tarde cuando el movimiento ya se extendio.",
                "Trader disciplinado: ejecuta solo si aparece confirmacion donde toca.",
            ]
        else:
            lines = [
                "Mentor:",
                "Lectura: el sesgo sigue siendo alcista, pero la entrada todavia necesita una confirmacion limpia para tener ventaja real.",
                "Confirmacion: busca rechazo alcista en retesteo o continuidad con debilidad vendedora.",
                "Invalidacion: si pierde estructura y sostiene por debajo, la idea pierde fuerza.",
                "Error comun: comprar por impulso solo porque la direccion ya es alcista.",
                "Trader disciplinado: espera el gatillo en zona valida; no persigas precio.",
            ]
        return "\n".join(lines)

    if direction == "BAJISTA":
        if strength == "FUERTE":
            lines = [
                "Mentor:",
                "Lectura: la estructura mantiene presion bajista y la ventaja aparece si el mercado confirma continuacion en zona.",
                "Confirmacion: retesteo fallido o nueva vela de impulso con rechazo comprador debil.",
                "Invalidacion: si rompe el ultimo techo operativo, la continuacion pierde valor.",
                "Error comun: entrar tarde cuando el movimiento ya se extendio.",
                "Trader disciplinado: ejecuta solo si aparece confirmacion donde toca.",
            ]
        else:
            lines = [
                "Mentor:",
                "Lectura: el sesgo sigue siendo bajista, pero la entrada todavia necesita una confirmacion limpia para tener ventaja real.",
                "Confirmacion: busca rechazo bajista en retesteo o continuidad con debilidad compradora.",
                "Invalidacion: si recupera estructura y sostiene arriba, la idea pierde fuerza.",
                "Error comun: vender por impulso solo porque la direccion ya es bajista.",
                "Trader disciplinado: espera el gatillo en zona valida; no persigas precio.",
            ]
        return "\n".join(lines)

    return (
        "Mentor:\n"
        "Lectura: la estructura no esta lo bastante clara para definir una ventaja operativa limpia.\n"
        "Confirmacion: espera una direccion clara antes de preparar entrada.\n"
        "Invalidacion: si el precio sigue mixto, la idea no merece ejecucion.\n"
        "Error comun: forzar una operacion cuando el contexto no esta ordenado.\n"
        "Trader disciplinado: sin claridad, no hay entrada."
    )


def _session_status_for_alert(alert_bar_utc: str) -> Tuple[str, str]:
    bar_dt = _parse_iso_utc(str(alert_bar_utc or "").strip())
    ref_dt = bar_dt if bar_dt is not None else datetime.now(pytz.UTC)
    ref_ny = ref_dt.astimezone(NY_TZ)
    weekday = ref_ny.weekday()

    if weekday >= 4:
        return "No favorable", "No operar"
    return "Optima", "Operar normal"


def _session_risk_note() -> str:
    return (
        "Nota de sesion: los viernes y durante el fin de semana el mercado suele mostrar menos liquidez, "
        "mas barridas y liquidaciones rapidas; si la estructura se ensucia, reduce riesgo o no operes."
    )


def _format_colombia_alert_time(alert_bar_utc: str) -> str:
    bar_dt = _parse_iso_utc(str(alert_bar_utc or "").strip())
    ref_dt = bar_dt if bar_dt is not None else datetime.now(pytz.UTC)
    return ref_dt.astimezone(BOGOTA_TZ).strftime("%Y-%m-%d %H:%M")


def _resolve_operational_trade_plan(
    estado: Dict[str, Any],
    compute_ctx: Dict[str, Any],
    lookback_bars: int = 5,
) -> Dict[str, Any]:
    direction = str(estado.get("direccion_v13", "")).upper().strip()
    entry_price = _safe_float(estado.get("precio_alerta"), 0.0)
    df_ind = compute_ctx.get("df_ind")
    base = {
        "ok": False,
        "reason": "plan_operativo_incompleto",
        "risk_pct": 0.0,
        "risk_pct_structural": 0.0,
        "sl_price": 0.0,
        "tp_price": 0.0,
        "rr_ratio": OPERATIONAL_TP_R_MULT,
        "lookback_bars": int(max(2, lookback_bars)),
        "adjusted_to_min": False,
        "structure_price": 0.0,
    }
    if direction not in {"ALCISTA", "BAJISTA"} or entry_price <= 0:
        return base
    if not isinstance(df_ind, pd.DataFrame) or df_ind.empty:
        base["reason"] = "plan_operativo_sin_df"
        return base
    if "High" not in df_ind.columns or "Low" not in df_ind.columns:
        base["reason"] = "plan_operativo_sin_estructura"
        return base

    window = max(2, min(int(lookback_bars or 5), len(df_ind)))
    highs = pd.to_numeric(df_ind["High"], errors="coerce").tail(window)
    lows = pd.to_numeric(df_ind["Low"], errors="coerce").tail(window)
    if highs.empty or lows.empty:
        base["reason"] = "plan_operativo_sin_estructura"
        return base

    if direction == "ALCISTA":
        structure_price = _safe_float(lows.min(), 0.0)
        if structure_price <= 0 or structure_price >= entry_price:
            base["reason"] = "plan_operativo_estructura_long_invalida"
            return base
        structural_risk_pct = ((entry_price - structure_price) / entry_price) * 100.0
    else:
        structure_price = _safe_float(highs.max(), 0.0)
        if structure_price <= entry_price:
            base["reason"] = "plan_operativo_estructura_short_invalida"
            return base
        structural_risk_pct = ((structure_price - entry_price) / entry_price) * 100.0

    structural_risk_pct = max(0.0, structural_risk_pct)
    risk_pct = structural_risk_pct

    base["risk_pct_structural"] = round(structural_risk_pct, 4)
    base["risk_pct"] = round(risk_pct, 4)
    base["adjusted_to_min"] = False
    base["structure_price"] = round(structure_price, 10)

    if risk_pct < OPERATIONAL_SL_MIN_PCT:
        base["reason"] = f"riesgo_operativo_fuera_marco(<{OPERATIONAL_SL_MIN_PCT:.2f}%)"
        return base

    if risk_pct > OPERATIONAL_SL_MAX_PCT:
        base["reason"] = f"riesgo_operativo_fuera_marco(>{OPERATIONAL_SL_MAX_PCT:.2f}%)"
        return base

    if direction == "ALCISTA":
        sl_price = entry_price * (1.0 - (risk_pct / 100.0))
        tp_price = entry_price * (1.0 + ((risk_pct * OPERATIONAL_TP_R_MULT) / 100.0))
    else:
        sl_price = entry_price * (1.0 + (risk_pct / 100.0))
        tp_price = entry_price * (1.0 - ((risk_pct * OPERATIONAL_TP_R_MULT) / 100.0))

    base.update(
        {
            "ok": True,
            "reason": "",
            "sl_price": round(sl_price, 10),
            "tp_price": round(tp_price, 10),
        }
    )
    return base


def _parse_mtf_summary_counts(summary: Any) -> Tuple[int, int, int]:
    text = str(summary or "").strip().lower()
    if not text:
        return 0, 0, 0

    def _extract(label: str) -> int:
        match = re.search(rf"{label}\s*=\s*(\d+)", text)
        if not match:
            return 0
        try:
            return int(match.group(1))
        except Exception:
            return 0

    return _extract("confirmaciones"), _extract("opuestos"), _extract("neutrales")


def _signal_strength_label(estado: Dict[str, Any], dorado: Dict[str, Any]) -> str:
    confidence = _safe_float(estado.get("confidence_score"), 0.0)
    min_conf = _safe_float(estado.get("min_confidence_required"), 0.0)
    rr = _safe_float(dorado.get("rr_estimado"), 0.0)
    micro_score = _safe_float(dorado.get("micro_score"), 0.0)
    umbral = _safe_float(dorado.get("umbral"), 0.0)
    pattern = str(estado.get("candle_pattern", "sin_patron")).strip().lower()
    riesgo = str(estado.get("riesgo", "")).strip().lower()
    session_state, _ = _session_status_for_alert(str(estado.get("indice_alerta_utc", "")).strip())
    confirmations, opposites, neutrals = _parse_mtf_summary_counts(estado.get("mtf_summary", ""))
    if (
        session_state == "Optima"
        and pattern != "sin_patron"
        and confidence >= max(min_conf + 8.0, 88.0)
        and rr >= 2.2
        and micro_score >= max(umbral + 1.0, 5.0)
        and riesgo not in {"alto", "muy alto"}
        and opposites == 0
        and confirmations >= 1
        and neutrals == 0
    ):
        return "FUERTE"
    return "DEBIL"


def _build_alert_payload(
    cfg: Dict[str, Any],
    item: MarketItem,
    estado: Dict[str, Any],
    source: str,
    structural_context_label: str | None = None,
) -> Tuple[str, str]:
    dorado = estado.get("dorado_v13") or {}
    score = dorado.get("micro_score")
    umbral = dorado.get("umbral")
    operational_plan = estado.get("operational_plan", {}) if isinstance(estado.get("operational_plan", {}), dict) else {}
    rr = dorado.get("rr_estimado", operational_plan.get("rr_ratio", OPERATIONAL_TP_R_MULT))
    direction = estado.get("direccion_v13", "")
    temporalidad = str(estado.get("temporalidad_alerta", "")).strip()
    modo = str(estado.get("modo_alerta", "Tendencial")).strip() or "Tendencial"
    if not temporalidad:
        temporalidad = "1D + 4H" if "1d+4h" in modo.lower().replace(" ", "") else str(cfg.get("interval", "15m"))

    rr_value = _safe_float(rr, 0.0)
    rr_text = f"1/{rr_value:.2f}" if rr_value > 0 else "N/A"
    score_text = str(score).strip() if score is not None else "N/A"
    umbral_text = str(umbral).strip() if umbral is not None else "N/A"
    precio_alerta_text = _format_price(estado.get("precio_alerta"))
    sl_text = _format_price(operational_plan.get("sl_price"))
    tp_text = _format_price(operational_plan.get("tp_price"))
    indice_alerta_utc = str(estado.get("indice_alerta_utc", "")).strip() or "N/A"
    confidence_text = str(estado.get("confidence_score", "N/A")).strip() or "N/A"
    pattern_text = str(estado.get("candle_pattern", "sin_patron")).strip()
    setup_tipo = str(estado.get("setup_tipo", dorado.get("setup_tipo", ""))).strip().lower()
    strength_label = _signal_strength_label(estado=estado, dorado=dorado)
    action_text = _alert_action_label(direction=direction, strength=strength_label)
    context_text = str(structural_context_label or estado.get("contexto_estructural", "")).strip()
    if not context_text:
        context_text = _structural_context_label_from_state(estado)
    if not context_text or context_text == "No claro":
        context_text = _human_direction_label(direction) if str(direction or "").upper().strip() in {"ALCISTA", "BAJISTA"} else "No claro"
    scenario_text = _alert_operational_scenario_label(
        direction=direction,
        setup_tipo=setup_tipo,
        strength=strength_label,
        temporalidad=temporalidad,
        modo=modo,
    )
    mentor_text = _mentor_block(direction=direction, strength=strength_label)
    session_state, session_recommendation = _session_status_for_alert(indice_alerta_utc)
    session_note = _session_risk_note()
    hora_col_text = str(estado.get("hora_col", "")).strip() or _format_colombia_alert_time(indice_alerta_utc)

    prefix = str(cfg.get("notification", {}).get("subject_prefix", "")).strip()
    subject_parts = [item.ticker, temporalidad]
    if prefix:
        subject_parts.insert(0, prefix)
    subject = " | ".join(subject_parts)

    body = (
        f"Estado de la sesion: {session_state}\n"
        f"Recomendacion: {session_recommendation}\n"
        f"Hora Col: {hora_col_text}\n"
        f"Contexto estructural: {context_text}\n"
        f"Direccion: {direction}\n"
        f"Accion: {action_text}\n"
        f"Escenario operativo: {scenario_text}\n"
        f"➡️ Entrada: {precio_alerta_text}\n"
        f"🟥 SL: {sl_text}\n"
        f"🟩 TP: {tp_text}\n"
        f"Fuerza: {strength_label}\n"
        f"Riesgo/beneficio: {rr_text}\n"
        f"Puntaje tecnico: {confidence_text}/100\n"
        f"Checklist tecnico: {score_text}/{umbral_text}\n"
        f"Patron: {pattern_text}\n\n"
        f"{mentor_text}\n\n"
        f"{session_note}"
    )
    return subject, body


def _build_replaced_payload(
    cfg: Dict[str, Any],
    item: MarketItem,
    replaced_alert: Dict[str, Any],
    estado: Dict[str, Any],
) -> Tuple[str, str]:
    prefix = str(cfg.get("notification", {}).get("subject_prefix", "")).strip()
    temporalidad = str(
        replaced_alert.get("interval")
        or estado.get("temporalidad_alerta")
        or estado.get("analysis_interval")
        or cfg.get("interval", "15m")
    ).strip()
    subject_parts = ["ALERTA REPLACED", item.ticker, temporalidad]
    if prefix:
        subject_parts.insert(0, prefix)
    subject = " | ".join(subject_parts)

    previous_direction = str(replaced_alert.get("direction", "") or "N/A").strip()
    previous_entry = _format_price(replaced_alert.get("entry_price"))
    opened_utc = str(replaced_alert.get("opened_utc", "") or "N/A").strip() or "N/A"
    closed_utc = str(replaced_alert.get("closed_utc", "") or "N/A").strip() or "N/A"
    previous_rr = _safe_float(replaced_alert.get("rr_estimado"), 0.0)
    rr_text = f"{previous_rr:.2f}" if previous_rr > 0 else "N/A"
    new_direction = str(estado.get("direccion_v13", "") or "N/A").strip()
    new_entry = _format_price(estado.get("precio_alerta"))
    new_bar_utc = str(estado.get("indice_alerta_utc", "") or "N/A").strip() or "N/A"

    body = (
        "La alerta anterior del mismo activo/timeframe fue reemplazada por una nueva lectura.\n"
        f"Direccion anterior: {previous_direction}\n"
        f"Entrada anterior: {previous_entry}\n"
        f"R:R anterior: {rr_text}\n"
        f"Abierta UTC: {opened_utc}\n"
        f"Replaced UTC: {closed_utc}\n"
        f"Nueva direccion: {new_direction}\n"
        f"Nueva entrada: {new_entry}\n"
        f"Nueva vela UTC: {new_bar_utc}\n"
        "Motivo: llego una nueva alerta antes de que la anterior cerrara por TP/SL/timeout."
    )
    return subject, body


def _analysis_mode(cfg: Dict[str, Any]) -> str:
    raw = str(cfg.get("analysis_mode", "tendencial") or "").strip().lower()
    if raw in {"estructural", "structural", "1d+4h", "1d_4h"}:
        return "estructural"
    return "tendencial"


def _compute_estado(
    item: MarketItem,
    cfg: Dict[str, Any],
    forced_mode: str | None = None,
    forced_interval: str | None = None,
) -> Tuple[Dict[str, Any] | None, str, str, Dict[str, Any]]:
    precision_cfg = _resolve_precision_cfg(cfg)
    require_closed_candle = bool(precision_cfg.get("require_closed_candle", True))
    grace_seconds = int(precision_cfg.get("closed_candle_grace_sec", 10))
    mode = str(forced_mode or _analysis_mode(cfg)).strip().lower()
    if mode == "estructural":
        df_1d, source_1d, err_1d = _fetch_data(item, period="3y", interval="1d", cfg=cfg)
        if df_1d is None or df_1d.empty:
            return None, "", f"1D: {err_1d or 'sin datos'}", {}

        df_4h, source_4h, err_4h = _fetch_data(item, period="12mo", interval="4h", cfg=cfg)
        if df_4h is None or df_4h.empty:
            return None, "", f"4H: {err_4h or 'sin datos'}", {}

        df_1d_ready, ok_1d, _ = _prepare_df_for_closed_candle(
            df_1d,
            interval="1d",
            require_closed_candle=require_closed_candle,
            grace_seconds=grace_seconds,
        )
        if not ok_1d or df_1d_ready.empty:
            return None, "", "1D sin velas cerradas suficientes.", {}

        df_4h_ready, ok_4h, trimmed_4h = _prepare_df_for_closed_candle(
            df_4h,
            interval="4h",
            require_closed_candle=require_closed_candle,
            grace_seconds=grace_seconds,
        )
        if not ok_4h or df_4h_ready.empty:
            return None, "", "4H sin velas cerradas suficientes.", {}

        try:
            df_1d_ind = calcular_indicadores(df_1d_ready)
            df_4h_ind = calcular_indicadores(df_4h_ready)
            estado = construir_estado_final_estructural(df_1d_ind, df_4h_ind, impacto_memoria=0)
        except Exception as exc:
            return None, "", f"Error estructural: {exc}", {}

        if not isinstance(estado, dict):
            return None, "", "Estado estructural invalido.", {}
        estado["modo_alerta"] = "Estructural (1D+4H)"
        estado["temporalidad_alerta"] = "1D + 4H"
        try:
            estado["precio_alerta"] = float(df_4h_ind["Close"].iloc[-1])
        except Exception:
            estado["precio_alerta"] = None
        estado["indice_alerta_utc"] = _index_to_iso_utc(df_4h_ind.index[-1]) if len(df_4h_ind.index) else ""
        vol_ratio = _atr_ratio_from_df(df_4h_ind)
        estado["vol_ratio"] = round(vol_ratio, 4)
        estado["analysis_interval"] = "4h"
        estado["open_candle_trimmed"] = bool(trimmed_4h)
        ctx = {
            "interval": "4h",
            "df_ind": df_4h_ind,
            "vol_ratio": vol_ratio,
            "open_candle_trimmed": bool(trimmed_4h),
        }
        return estado, f"1D:{source_1d} | 4H:{source_4h}", "", ctx

    interval = str(forced_interval or cfg.get("interval", "15m")).strip().lower()
    if interval not in TD_INTERVAL_MAP:
        interval = "15m"
    period = str(cfg.get("period", "5d"))
    df, source, fetch_err = _fetch_data(item, period=period, interval=interval, cfg=cfg)
    if df is None or df.empty:
        return None, "", fetch_err or "No se pudieron descargar velas.", {}

    df_ready, ready_ok, trimmed_open = _prepare_df_for_closed_candle(
        df,
        interval=interval,
        require_closed_candle=require_closed_candle,
        grace_seconds=grace_seconds,
    )
    if not ready_ok or df_ready.empty:
        return None, "", "Sin velas cerradas suficientes para analisis.", {}

    try:
        df_ind = calcular_indicadores(df_ready)
        estado = construir_estado_final(df_ind, impacto_memoria=0)
    except Exception as exc:
        return None, "", f"Error calculando estado: {exc}", {}

    if not isinstance(estado, dict):
        return None, "", "Estado tendencial invalido.", {}
    estado["modo_alerta"] = "Tendencial"
    estado["temporalidad_alerta"] = interval
    try:
        estado["precio_alerta"] = float(df_ind["Close"].iloc[-1])
    except Exception:
        estado["precio_alerta"] = None
    estado["indice_alerta_utc"] = _index_to_iso_utc(df_ind.index[-1]) if len(df_ind.index) else ""
    vol_ratio = _atr_ratio_from_df(df_ind)
    estado["vol_ratio"] = round(vol_ratio, 4)
    estado["analysis_interval"] = interval
    estado["open_candle_trimmed"] = bool(trimmed_open)
    ctx = {
        "interval": interval,
        "df_ind": df_ind,
        "vol_ratio": vol_ratio,
        "open_candle_trimmed": bool(trimmed_open),
    }
    return estado, source, "", ctx


def _send_email_alert(
    cfg: Dict[str, Any],
    item: MarketItem,
    subject: str,
    body: str,
    recipients_override: List[str] | None = None,
) -> Tuple[bool, str]:
    recipients = [str(x).strip() for x in (recipients_override or []) if str(x).strip()]
    if not recipients:
        recipients = _parse_recipients(cfg)
    if not recipients:
        return False, "Sin destinatarios (notification.email.to / notification.email_to / ALERT_EMAIL_TO)."

    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com").strip()
    smtp_port_raw = os.getenv("SMTP_PORT", "587").strip()
    smtp_user = os.getenv("SMTP_USER", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "").strip()
    smtp_from = os.getenv("SMTP_FROM", smtp_user).strip()
    if not smtp_user or not smtp_password or not smtp_from:
        return False, "SMTP no configurado (SMTP_USER/SMTP_PASSWORD/SMTP_FROM)."

    try:
        smtp_port = int(smtp_port_raw)
    except Exception:
        smtp_port = 587

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
            server.ehlo()
            if smtp_port == 587:
                server.starttls()
                server.ehlo()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        return True, ""
    except Exception as exc:
        return False, _redact_text(str(exc))


def _send_telegram_alert(
    cfg: Dict[str, Any],
    subject: str,
    body: str,
    chat_ids_override: List[str] | None = None,
    item: MarketItem | None = None,
) -> Tuple[bool, str]:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        return False, "TELEGRAM_BOT_TOKEN no configurado."

    chat_ids = [str(x).strip() for x in (chat_ids_override or []) if str(x).strip()]
    if not chat_ids:
        chat_ids = _parse_telegram_chat_ids(cfg)
    if not chat_ids:
        return False, "Sin chat_ids (notification.telegram.chat_ids / ALERT_TELEGRAM_CHAT_IDS)."

    parse_mode = str((cfg.get("notification", {}).get("telegram", {}) or {}).get("parse_mode", "")).strip()
    message = f"{subject}\n\n{body}"
    caption = message if len(message) <= 1024 else f"{message[:1000]}..."
    photo_url = _coin_image_url_for_telegram(cfg, item)

    errors = []
    sent_any = False
    for chat_id in chat_ids:
        try:
            photo_error = ""
            if photo_url:
                photo_payload: Dict[str, Any] = {"chat_id": chat_id, "photo": photo_url, "caption": caption}
                if parse_mode:
                    photo_payload["parse_mode"] = parse_mode
                photo_resp = requests.post(
                    f"https://api.telegram.org/bot{token}/sendPhoto",
                    data=photo_payload,
                    timeout=20,
                )
                if photo_resp.status_code < 400:
                    photo_data = photo_resp.json()
                    if photo_data.get("ok", False):
                        sent_any = True
                        continue
                    photo_error = str(photo_data.get("description", "Error Telegram sin descripcion"))
                else:
                    photo_error = f"HTTP {photo_resp.status_code}"

            payload: Dict[str, Any] = {"chat_id": chat_id, "text": message}
            if parse_mode:
                payload["parse_mode"] = parse_mode
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=payload,
                timeout=20,
            )
            if resp.status_code >= 400:
                if photo_error:
                    errors.append(f"{chat_id}: foto({photo_error}) | mensaje(HTTP {resp.status_code})")
                else:
                    errors.append(f"{chat_id}: HTTP {resp.status_code}")
                continue
            data = resp.json()
            if not data.get("ok", False):
                msg_err = str(data.get("description", "Error Telegram sin descripcion"))
                if photo_error:
                    errors.append(f"{chat_id}: foto({photo_error}) | mensaje({msg_err})")
                else:
                    errors.append(f"{chat_id}: {msg_err}")
                continue
            sent_any = True
        except Exception as exc:
            errors.append(f"{chat_id}: {exc}")

    if sent_any:
        return True, ""
    return False, _redact_text("; ".join(errors) if errors else "No se pudo enviar a Telegram.")


def _send_windows_toast(subject: str, body: str) -> Tuple[bool, str]:
    if os.name != "nt":
        return False, "Windows toast solo disponible en Windows."

    # Escape simple para comillas simples en PowerShell.
    safe_title = subject.replace("'", "''")
    safe_body = body.replace("'", "''")
    script = (
        "[Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] > $null;"
        "[Windows.UI.Notifications.ToastNotification, Windows.UI.Notifications, ContentType = WindowsRuntime] > $null;"
        "[Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType = WindowsRuntime] > $null;"
        "$template = [Windows.UI.Notifications.ToastTemplateType]::ToastText02;"
        "$xml = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent($template);"
        "$texts = $xml.GetElementsByTagName('text');"
        "$texts.Item(0).AppendChild($xml.CreateTextNode('{title}')) > $null;"
        "$texts.Item(1).AppendChild($xml.CreateTextNode('{body}')) > $null;"
        "$toast = [Windows.UI.Notifications.ToastNotification]::new($xml);"
        "$notifier = [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier('Estrella Trader');"
        "$notifier.Show($toast);"
    ).format(title=safe_title[:180], body=safe_body[:350])

    try:
        proc = subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                script,
            ],
            capture_output=True,
            text=True,
            timeout=12,
            check=False,
        )
        if proc.returncode != 0:
            stderr = (proc.stderr or "").strip()
            stdout = (proc.stdout or "").strip()
            return False, _redact_text(stderr or stdout or f"powershell returncode={proc.returncode}")
        return True, ""
    except Exception as exc:
        return False, _redact_text(str(exc))


def _channel_enabled(cfg: Dict[str, Any], channel: str, default: bool = True) -> bool:
    notif = cfg.get("notification", {}) if isinstance(cfg.get("notification", {}), dict) else {}
    ch = notif.get(channel, {})
    if not isinstance(ch, dict):
        return default
    raw = ch.get("enabled", default)
    return bool(raw)


def _apply_precision_filters(
    item: MarketItem,
    cfg: Dict[str, Any],
    estado: Dict[str, Any],
    compute_ctx: Dict[str, Any],
    mtf_cache: Dict[str, Dict[str, Any]],
    precision_cfg: Dict[str, Any] | None = None,
    record: Dict[str, Any] | None = None,
    record_key: str = "",
) -> Dict[str, Any]:
    if not isinstance(precision_cfg, dict):
        precision_cfg = _resolve_precision_cfg(cfg)

    effective_cfg = dict(precision_cfg)
    if isinstance(record, dict):
        effective_cfg = _apply_record_quality_calibration(
            precision_cfg=effective_cfg,
            record=record,
            record_key=record_key,
        )
        if not isinstance(effective_cfg.get("quality_metrics"), dict):
            effective_cfg["quality_metrics"] = _quality_metrics_from_record(record)

    enabled = bool(effective_cfg.get("enabled", True))

    dorado = estado.get("dorado_v13") or {}
    dorado_now = bool(dorado)
    setup_tipo = str(estado.get("setup_tipo", dorado.get("setup_tipo", ""))).strip().lower()
    direction = str(estado.get("direccion_v13", "NEUTRAL")).upper().strip() or "NEUTRAL"
    interval = str(compute_ctx.get("interval") or estado.get("analysis_interval") or cfg.get("interval", "15m")).strip()
    vol_ratio = _safe_float(compute_ctx.get("vol_ratio", estado.get("vol_ratio", 1.0)), 1.0)
    df_ind = compute_ctx.get("df_ind")
    rr = _safe_float(dorado.get("rr_estimado"), 0.0)
    operational_plan = _resolve_operational_trade_plan(estado=estado, compute_ctx=compute_ctx)
    risk_ok = bool(operational_plan.get("ok", False))

    mtf_enabled = enabled and bool(effective_cfg.get("multi_timeframe_filter", True))
    if mtf_enabled:
        mtf_info = _evaluate_mtf_alignment(
            item=item,
            base_interval=interval,
            base_direction=direction,
            precision_cfg=effective_cfg,
            cache=mtf_cache,
            cfg=cfg,
        )
    else:
        mtf_info = {
            "ok": True,
            "score": 12,
            "details": [],
            "summary": "Filtro MTF deshabilitado.",
            "confirmations": 0,
            "opposites": 0,
        }

    candle_info = _detect_price_action(df_ind, direction)
    require_price_action = enabled and bool(effective_cfg.get("require_price_action_confirmation", True))
    candle_ok = bool(candle_info.get("aligned", False)) if require_price_action else (str(candle_info.get("bias")) != _opposite_direction(direction))

    rr_min = max(2.0, _safe_float(effective_cfg.get("min_rr", 1.8), 1.8)) if enabled else 0.0
    rr_ok = rr >= rr_min

    confidence_score = _compute_signal_confidence(estado=estado, mtf_info=mtf_info, candle_info=candle_info)
    min_confidence = _compute_dynamic_min_confidence(
        base_interval=interval,
        vol_ratio=vol_ratio,
        precision_cfg=effective_cfg,
    ) if enabled else 0
    confidence_ok = confidence_score >= min_confidence
    pullback_neutral_mtf_ok = enabled and _should_allow_pullback_neutral_mtf(
        setup_tipo=setup_tipo,
        base_interval=interval,
        rr=rr,
        mtf_info=mtf_info,
        confidence_score=confidence_score,
        min_confidence=min_confidence,
    )
    if pullback_neutral_mtf_ok:
        mtf_info = dict(mtf_info)
        mtf_info["ok"] = True
        mtf_info["override"] = "pullback_neutral"
        summary = str(mtf_info.get("summary", "")).strip()
        mtf_info["summary"] = (
            f"{summary} | override=pullback_neutral"
            if summary
            else "override=pullback_neutral"
        )
        confidence_ok = True

    cooldown_effective = _compute_adaptive_cooldown_minutes(
        cfg=cfg,
        base_interval=interval,
        vol_ratio=vol_ratio,
        precision_cfg=effective_cfg,
    ) if enabled else max(1, int(cfg.get("cooldown_minutes", 60)))

    mtf_ok = bool(mtf_info.get("ok", True))
    signal_ready = dorado_now and risk_ok and (not enabled or (mtf_ok and candle_ok and rr_ok and confidence_ok))

    reasons: List[str] = []
    if not dorado_now:
        reasons.append("dorado_inactivo")
    if enabled and not mtf_ok:
        reasons.append("mtf_no_alineado")
    if enabled and not candle_ok:
        reasons.append("vela_sin_confirmacion")
    if enabled and not rr_ok:
        reasons.append(f"rr_bajo(<{rr_min})")
    if enabled and not confidence_ok:
        reasons.append(f"confianza_baja(<{min_confidence})")
    if not risk_ok:
        reasons.append(str(operational_plan.get("reason", "plan_operativo_invalido")))
    if not reasons:
        reasons.append("filtros_ok")

    thresholds = {
        "min_confidence_base": int(effective_cfg.get("min_confidence_score", 0) or 0),
        "min_confidence_effective": int(min_confidence or 0),
        "min_rr": round(rr_min, 3),
        "min_mtf_confirmations": max(1, int(effective_cfg.get("min_mtf_confirmations", 1) or 1)),
        "cooldown_minutes": int(cooldown_effective or 0),
    }

    return {
        "enabled": enabled,
        "signal_ready": signal_ready,
        "dorado_now": dorado_now,
        "profile": str(effective_cfg.get("alert_profile", "balanceado")),
        "calibration": dict(effective_cfg.get("quality_calibration", {}))
        if isinstance(effective_cfg.get("quality_calibration", {}), dict)
        else {},
        "quality_metrics": dict(effective_cfg.get("quality_metrics", {}))
        if isinstance(effective_cfg.get("quality_metrics", {}), dict)
        else {},
        "thresholds": thresholds,
        "confidence_score": confidence_score,
        "min_confidence": min_confidence,
        "rr": rr,
        "min_rr": rr_min,
        "rr_ok": rr_ok,
        "operational_plan": operational_plan,
        "risk_ok": risk_ok,
        "mtf": mtf_info,
        "candle": candle_info,
        "cooldown_minutes": cooldown_effective,
        "reasons": reasons,
        "interval": interval,
        "vol_ratio": vol_ratio,
    }


def _resolve_scan_targets(cfg: Dict[str, Any]) -> List[Dict[str, str]]:
    targets: List[Dict[str, str]] = []
    auto_multi = bool(cfg.get("auto_multi_interval", True))

    if auto_multi:
        raw_intervals = cfg.get("scan_intervals", ["15m", "30m", "1h", "4h"])
        if not isinstance(raw_intervals, list):
            raw_intervals = ["15m", "30m", "1h", "4h"]
        seen = set()
        for raw in raw_intervals:
            iv = str(raw).strip().lower()
            if iv not in TD_INTERVAL_MAP:
                continue
            if iv in seen:
                continue
            seen.add(iv)
            targets.append(
                {
                    "mode": "tendencial",
                    "interval": iv,
                    "key": iv,
                    "label": iv,
                }
            )
    else:
        mode = _analysis_mode(cfg)
        if mode == "estructural":
            targets.append(
                {
                    "mode": "estructural",
                    "interval": "1D + 4H",
                    "key": "1d_4h",
                    "label": "1D + 4H",
                }
            )
        else:
            iv = str(cfg.get("interval", "15m")).strip().lower()
            if iv not in TD_INTERVAL_MAP:
                iv = "15m"
            targets.append(
                {
                    "mode": "tendencial",
                    "interval": iv,
                    "key": iv,
                    "label": iv,
                }
            )

    if bool(cfg.get("scan_structural_1d_4h", True)):
        if not any(str(t.get("mode", "")).lower() == "estructural" for t in targets):
            targets.append(
                {
                    "mode": "estructural",
                    "interval": "1D + 4H",
                    "key": "1d_4h",
                    "label": "1D + 4H",
                }
            )

    if not targets:
        targets.append(
            {
                "mode": "tendencial",
                "interval": "15m",
                "key": "15m",
                "label": "15m",
            }
        )
    return targets


def run_scan_cycle(cfg: Dict[str, Any], state: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    cycle_started_perf = time.perf_counter()
    cycle_metrics = _new_cycle_metrics()
    cfg = _apply_resource_profile(cfg)
    compact_mode = _resource_profile_active(cfg, "render_512mb")
    cycle_metrics["rss_start_mb"] = _current_process_rss_mb()
    cycle_metrics["rss_peak_mb"] = float(cycle_metrics.get("rss_start_mb", 0.0) or 0.0)
    precision_cfg = _resolve_effective_precision_cfg(cfg, state)
    persistence_bars = max(1, int(precision_cfg.get("persistence_bars", 1)))
    max_alerts_per_symbol_day = max(0, int(precision_cfg.get("max_alerts_per_symbol_day", 0)))
    watchlist = _build_watchlist(cfg)
    scan_targets = _resolve_scan_targets(cfg)
    cycle_metrics["watchlist_size"] = len(watchlist)
    cycle_metrics["scan_targets"] = len(scan_targets)
    cal = precision_cfg.get("quality_calibration", {})
    if isinstance(cal, dict):
        logging.debug(
            "Precision profile=%s | cal_mode=%s | resolved=%s | acc=%.2f | noise=%.2f",
            precision_cfg.get("alert_profile", "balanceado"),
            cal.get("mode", "n/a"),
            int(cal.get("resolved", 0) or 0),
            _safe_float(cal.get("accuracy_pct"), 0.0),
            _safe_float(cal.get("noise_pct"), 0.0),
        )

    if not watchlist:
        logging.warning("Watchlist vacia. Revisa scanner_config.json")
        rss_end = _current_process_rss_mb()
        cycle_metrics["rss_end_mb"] = rss_end
        cycle_metrics["rss_peak_mb"] = max(float(cycle_metrics.get("rss_peak_mb", 0.0) or 0.0), rss_end)
        cycle_metrics["duration_ms"] = round((time.perf_counter() - cycle_started_perf) * 1000.0, 3)
        return state, cycle_metrics

    symbols_state: Dict[str, Any] = state.setdefault("symbols", {})
    user_records = _load_user_records()
    configured_telegram_chat_ids = _dedupe_chat_ids(_parse_telegram_chat_ids(cfg))
    auto_telegram_chat_ids: List[str] = []
    telegram_user_chat_links = _normalize_telegram_user_chat_links(state)
    if _channel_enabled(cfg, "telegram", default=True):
        auto_telegram_chat_ids, telegram_user_chat_links, discover_err = _discover_telegram_chats_from_updates(
            state,
            user_records=user_records,
        )
        if discover_err:
            logging.warning("Telegram getUpdates: %s", discover_err)
    telegram_broadcast_ids = _dedupe_chat_ids(configured_telegram_chat_ids + auto_telegram_chat_ids)
    user_targets = _load_user_targets(telegram_user_chat_links=telegram_user_chat_links)
    mtf_cache: Dict[str, Dict[str, Any]] = {}
    structural_context_cache: Dict[str, str] = {}

    def _timed_send(channel: str, fn, *args, **kwargs) -> Tuple[bool, str]:
        started = time.perf_counter()
        ok, err = fn(*args, **kwargs)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        _record_notification_metrics(cycle_metrics, channel, bool(ok), elapsed_ms)
        return ok, err

    for item in watchlist:
        if not _market_open(item.kind):
            logging.debug("[%s] Mercado cerrado, se omite ciclo.", item.state_key)
            for target in scan_targets:
                record_key = f"{item.state_key}|{target['key']}"
                record = symbols_state.get(record_key, {})
                record["last_checked_utc"] = _now_iso_utc()
                record["market_open"] = False
                record["scan_target"] = target.get("label", target.get("interval", ""))
                record["last_error"] = ""
                symbols_state[record_key] = record
            continue

        for target in scan_targets:
            target_mode = str(target.get("mode", "tendencial")).strip().lower()
            target_interval = str(target.get("interval", "15m")).strip()
            target_key = str(target.get("key", target_interval)).strip().lower() or target_interval.lower()
            record_key = f"{item.state_key}|{target_key}"
            cycle_metrics["records_total"] = int(cycle_metrics.get("records_total", 0) or 0) + 1
            record_eval_started = time.perf_counter()

            record = symbols_state.get(record_key, {})
            record["last_checked_utc"] = _now_iso_utc()
            record["market_open"] = True
            record["scan_target"] = target.get("label", target_interval)

            estado, source, compute_err, compute_ctx = _compute_estado(
                item=item,
                cfg=cfg,
                forced_mode=target_mode,
                forced_interval=target_interval if target_mode != "estructural" else None,
            )
            if estado is None:
                record["last_error"] = _redact_text(compute_err or "No se pudo calcular estado.")
                symbols_state[record_key] = record
                cycle_metrics["records_error"] = int(cycle_metrics.get("records_error", 0) or 0) + 1
                cycle_metrics.setdefault("errors", []).append(f"{record_key}: {record['last_error']}")
                _observe_latency(cycle_metrics.get("latency_ms", {}).get("record_eval", {}), (time.perf_counter() - record_eval_started) * 1000.0)
                logging.warning("[%s|%s] Error de datos/estado: %s", item.state_key, target_key, record["last_error"])
                continue

            _evaluate_open_alert_outcome(record=record, estado=estado, compute_ctx=compute_ctx)
            precision = _apply_precision_filters(
                item=item,
                cfg=cfg,
                estado=estado,
                compute_ctx=compute_ctx,
                mtf_cache=mtf_cache,
                precision_cfg=precision_cfg,
                record=record,
                record_key=record_key,
            )
            signal_ready = bool(precision.get("signal_ready", False))
            signal_bar_utc = str(estado.get("indice_alerta_utc", "")).strip()
            prev_gate_bar = str(record.get("last_gate_bar_utc", "")).strip()
            prev_streak = int(record.get("dorado_streak", 0) or 0)

            if signal_ready:
                if signal_bar_utc and signal_bar_utc != prev_gate_bar:
                    current_streak = prev_streak + 1
                elif signal_bar_utc and signal_bar_utc == prev_gate_bar:
                    current_streak = prev_streak
                else:
                    current_streak = prev_streak + 1
            else:
                current_streak = 0

            record["dorado_streak"] = current_streak
            if signal_bar_utc:
                record["last_gate_bar_utc"] = signal_bar_utc

            estado["confidence_score"] = precision.get("confidence_score")
            estado["min_confidence_required"] = precision.get("min_confidence")
            estado["mtf_summary"] = precision.get("mtf", {}).get("summary", "")
            estado["mtf_details"] = precision.get("mtf", {}).get("details", [])
            estado["candle_pattern"] = precision.get("candle", {}).get("pattern", "sin_patron")
            estado["candle_pattern_desc"] = precision.get("candle", {}).get("description", "")
            estado["cooldown_minutes_effective"] = precision.get("cooldown_minutes")
            estado["vol_ratio"] = round(_safe_float(precision.get("vol_ratio", estado.get("vol_ratio", 1.0)), 1.0), 4)
            estado["filtros_precision"] = precision.get("reasons", [])
            estado["alert_profile"] = precision.get("profile", "balanceado")
            estado["quality_calibration"] = precision.get("calibration", {})
            estado["quality_metrics"] = precision.get("quality_metrics", {})
            estado["effective_thresholds"] = precision.get("thresholds", {})
            estado["hora_col"] = _format_colombia_alert_time(signal_bar_utc)
            estado["operational_plan"] = precision.get("operational_plan", {})
            estado["risk_pct_operativo"] = precision.get("operational_plan", {}).get("risk_pct")
            estado["sl_price_operativo"] = precision.get("operational_plan", {}).get("sl_price")
            estado["tp_price_operativo"] = precision.get("operational_plan", {}).get("tp_price")
            record["last_gate_reasons"] = precision.get("reasons", [])
            record["alert_profile"] = precision.get("profile", "balanceado")
            record["quality_calibration"] = precision.get("calibration", {})
            record["quality_metrics"] = precision.get("quality_metrics", {})
            record["effective_thresholds"] = precision.get("thresholds", {})
            record["last_confidence_score"] = precision.get("confidence_score")
            record["last_rr"] = precision.get("rr")
            record["last_operational_risk_pct"] = precision.get("operational_plan", {}).get("risk_pct")
            record["last_candle_pattern"] = precision.get("candle", {}).get("pattern", "sin_patron")
            record["last_mtf_summary"] = precision.get("mtf", {}).get("summary", "")
            quality_stats = record.get("quality_stats", {}) if isinstance(record.get("quality_stats", {}), dict) else {}
            estado["quality_accuracy_pct"] = quality_stats.get("accuracy_pct", 0.0)
            estado["quality_resolved_alerts"] = int(quality_stats.get("resolved", 0) or 0)

            if _should_alert(
                record=record,
                signal_ready=signal_ready,
                cooldown_minutes=int(precision.get("cooldown_minutes", cfg.get("cooldown_minutes", 60))),
                persistence_bars=persistence_bars,
                max_alerts_per_symbol_day=max_alerts_per_symbol_day,
            ):
                cycle_metrics["alerts_triggered"] = int(cycle_metrics.get("alerts_triggered", 0) or 0) + 1
                structural_context_label = _resolve_structural_context_label(
                    item=item,
                    estado=estado,
                    cfg=cfg,
                    cache=structural_context_cache,
                )
                estado["contexto_estructural"] = structural_context_label
                subject, body = _build_alert_payload(
                    cfg,
                    item,
                    estado,
                    source,
                    structural_context_label=structural_context_label,
                )
                notify_status: Dict[str, Any] = {}
                sent_any = False
                errors: List[str] = []
                enabled_any = False
                replacement_telegram_chat_ids: List[str] = []
                temporalidad = str(estado.get("temporalidad_alerta", target_interval)).strip()
                market_key = f"{item.market}|{item.label}|{temporalidad}"
                telegram_enabled = _channel_enabled(cfg, "telegram", default=True)

                if user_targets:
                    user_results: List[Dict[str, Any]] = []
                    premium_users_alerted = False
                    eligible_users = 0

                    email_enabled = _channel_enabled(cfg, "email", default=True)
                    windows_enabled = _channel_enabled(cfg, "windows", default=True)

                    if telegram_enabled:
                        enabled_any = True
                    if email_enabled:
                        enabled_any = True
                    if windows_enabled:
                        enabled_any = True

                    for target_user in user_targets:
                        user_id = target_user["id"]
                        chat_id = target_user["chat_id"]
                        email = target_user.get("email", "")
                        is_premium = bool(target_user.get("es_premium", False))

                        if not is_premium and not _free_user_can_receive_market_alert(state, user_id, market_key):
                            continue

                        eligible_users += 1
                        user_notified = False
                        user_errors: List[str] = []

                        if telegram_enabled and _chat_id_telegram_valido(chat_id):
                            sent_ok, sent_err = _timed_send(
                                "telegram",
                                _send_telegram_alert,
                                cfg,
                                subject,
                                body,
                                chat_ids_override=[chat_id],
                                item=item,
                            )
                            if sent_ok:
                                user_notified = True
                                sent_any = True
                                replacement_telegram_chat_ids.append(chat_id)
                            else:
                                user_errors.append(f"telegram: {sent_err}")

                        if is_premium and email_enabled and email:
                            sent_ok, sent_err = _timed_send(
                                "email",
                                _send_email_alert,
                                cfg,
                                item,
                                subject,
                                body,
                                recipients_override=[email],
                            )
                            if sent_ok:
                                user_notified = True
                                sent_any = True
                            else:
                                user_errors.append(f"email: {sent_err}")

                        if user_notified and is_premium:
                            premium_users_alerted = True

                        if user_notified and not is_premium:
                            _mark_free_user_market_alert(state, user_id, market_key)

                        user_results.append(
                            {
                                "user_id": user_id,
                                "premium": is_premium,
                                "ok": user_notified,
                                "error": "; ".join(user_errors),
                            }
                        )
                        if user_errors and not user_notified:
                            errors.append(f"{user_id}: {'; '.join(user_errors)}")

                    notify_status["users"] = user_results
                    known_user_chat_ids = _dedupe_chat_ids([t.get("chat_id", "") for t in user_targets])
                    broadcast_extra_chat_ids = [cid for cid in telegram_broadcast_ids if cid not in set(known_user_chat_ids)]
                    if telegram_enabled and broadcast_extra_chat_ids:
                        sent_ok, sent_err = _timed_send(
                            "telegram",
                            _send_telegram_alert,
                            cfg,
                            subject,
                            body,
                            chat_ids_override=broadcast_extra_chat_ids,
                            item=item,
                        )
                        notify_status["telegram_broadcast"] = {
                            "ok": sent_ok,
                            "error": sent_err,
                            "sent_to": len(broadcast_extra_chat_ids),
                        }
                        if sent_ok:
                            sent_any = True
                            replacement_telegram_chat_ids.extend(broadcast_extra_chat_ids)
                        else:
                            errors.append(f"telegram_broadcast: {sent_err}")

                    if eligible_users == 0:
                        errors.append("Sin usuarios elegibles con Telegram para esta alerta.")

                    if windows_enabled:
                        if premium_users_alerted:
                            sent_ok, sent_err = _timed_send("windows", _send_windows_toast, subject, body)
                            notify_status["windows"] = {"ok": sent_ok, "error": sent_err}
                            if sent_ok:
                                sent_any = True
                            else:
                                errors.append(f"windows: {sent_err}")
                        else:
                            notify_status["windows"] = {
                                "ok": False,
                                "error": "Sin usuarios premium alertados en este evento.",
                            }
                else:
                    if _channel_enabled(cfg, "email", default=True):
                        enabled_any = True
                        sent_ok, sent_err = _timed_send("email", _send_email_alert, cfg, item, subject, body)
                        notify_status["email"] = {"ok": sent_ok, "error": sent_err}
                        if sent_ok:
                            sent_any = True
                        else:
                            errors.append(f"email: {sent_err}")

                    if telegram_enabled:
                        enabled_any = True
                        sent_ok, sent_err = _timed_send(
                            "telegram",
                            _send_telegram_alert,
                            cfg,
                            subject,
                            body,
                            chat_ids_override=telegram_broadcast_ids or None,
                            item=item,
                        )
                        notify_status["telegram"] = {"ok": sent_ok, "error": sent_err}
                        if sent_ok:
                            sent_any = True
                            replacement_telegram_chat_ids.extend(telegram_broadcast_ids)
                        else:
                            errors.append(f"telegram: {sent_err}")

                    if _channel_enabled(cfg, "windows", default=True):
                        enabled_any = True
                        sent_ok, sent_err = _timed_send("windows", _send_windows_toast, subject, body)
                        notify_status["windows"] = {"ok": sent_ok, "error": sent_err}
                        if sent_ok:
                            sent_any = True
                        else:
                            errors.append(f"windows: {sent_err}")

                    if not enabled_any:
                        errors.append("No hay canales habilitados en notification.")

                record["last_notify"] = notify_status
                if sent_any:
                    cycle_metrics["alerts_sent"] = int(cycle_metrics.get("alerts_sent", 0) or 0) + 1
                    record["last_alert_utc"] = _now_iso_utc()
                    _increment_daily_alert_count(record)
                    snapshot = _open_alert_snapshot(
                        estado=estado,
                        precision=precision,
                        compute_ctx=compute_ctx,
                        precision_cfg=precision_cfg,
                    )
                    if snapshot is not None:
                        existing_open = record.get("open_alert")
                        if isinstance(existing_open, dict) and str(existing_open.get("status", "")).strip().lower() == "open":
                            existing_open["status"] = "replaced"
                            existing_open["closed_utc"] = _now_iso_utc()
                            existing_open["outcome_note"] = "reemplazada_por_nueva_alerta"
                            _append_quality_event(record, dict(existing_open))
                            _update_quality_stats(record, "replaced")
                            replacement_chat_ids = _dedupe_chat_ids(replacement_telegram_chat_ids)
                            if telegram_enabled and replacement_chat_ids:
                                replaced_subject, replaced_body = _build_replaced_payload(
                                    cfg=cfg,
                                    item=item,
                                    replaced_alert=existing_open,
                                    estado=estado,
                                )
                                replaced_ok, replaced_err = _timed_send(
                                    "telegram",
                                    _send_telegram_alert,
                                    cfg,
                                    replaced_subject,
                                    replaced_body,
                                    chat_ids_override=replacement_chat_ids,
                                    item=None,
                                )
                                notify_status["telegram_replaced"] = {
                                    "ok": replaced_ok,
                                    "error": replaced_err,
                                    "sent_to": len(replacement_chat_ids),
                                }
                                if not replaced_ok:
                                    logging.warning(
                                        "[%s|%s] Fallo aviso REPLACED: %s",
                                        item.state_key,
                                        target_key,
                                        replaced_err,
                                    )
                        record["open_alert"] = snapshot
                    logging.info("[%s|%s] Alerta enviada (multicanal).", item.state_key, target_key)
                else:
                    cycle_metrics["alerts_failed"] = int(cycle_metrics.get("alerts_failed", 0) or 0) + 1
                    record["last_error"] = _redact_text("; ".join(errors))
                    cycle_metrics.setdefault("errors", []).append(f"{record_key}: {record['last_error']}")
                    logging.error("[%s|%s] Fallo envio alerta: %s", item.state_key, target_key, record["last_error"])

            record["dorado_active"] = _compute_dorado_active_state(
                signal_ready=signal_ready,
                current_streak=current_streak,
                persistence_bars=persistence_bars,
            )
            record["last_source"] = source
            record["decision"] = estado.get("decision", "")
            record["riesgo"] = estado.get("riesgo", "")
            record["direccion"] = estado.get("direccion_v13", "")
            record["analysis_interval"] = precision.get("interval")
            record["cooldown_minutes_effective"] = precision.get("cooldown_minutes")
            record["daily_alert_count"] = _daily_alert_count(record)
            if not record.get("last_error"):
                record["last_error"] = ""
            symbols_state[record_key] = record
            cycle_metrics["records_success"] = int(cycle_metrics.get("records_success", 0) or 0) + 1
            _observe_latency(cycle_metrics.get("latency_ms", {}).get("record_eval", {}), (time.perf_counter() - record_eval_started) * 1000.0)
            if compact_mode and int(cycle_metrics.get("records_total", 0) or 0) % 8 == 0:
                _compact_memory_sweep()

    if compact_mode:
        _compact_memory_sweep()
    rss_end = _current_process_rss_mb()
    cycle_metrics["rss_end_mb"] = rss_end
    cycle_metrics["rss_peak_mb"] = max(float(cycle_metrics.get("rss_peak_mb", 0.0) or 0.0), rss_end)
    cycle_metrics["duration_ms"] = round((time.perf_counter() - cycle_started_perf) * 1000.0, 3)
    return state, cycle_metrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Estrella Trader market scanner worker")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Ruta al scanner_config.json")
    parser.add_argument("--state", default=str(DEFAULT_STATE_PATH), help="Ruta al scanner_state.json")
    parser.add_argument("--log", default=str(DEFAULT_LOG_PATH), help="Ruta al scanner.log")
    parser.add_argument("--health", default=str(DEFAULT_HEALTH_PATH), help="Ruta al scanner_health.json")
    parser.add_argument("--once", action="store_true", help="Ejecuta un solo ciclo y termina.")
    parser.add_argument("--debug", action="store_true", help="Activa logs DEBUG.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config).resolve()
    state_path = Path(args.state).resolve()
    log_path = Path(args.log).resolve()
    health_path = Path(args.health).resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    health_path.parent.mkdir(parents=True, exist_ok=True)

    _setup_logging(log_path, debug=bool(args.debug))
    lock_ok, lock_msg = _acquire_single_instance_lock(LOCK_PATH)
    if not lock_ok:
        logging.warning(lock_msg)
        return 0
    logging.info("Scanner worker iniciado.")

    try:
        cfg = load_config(config_path)
        state = load_state(state_path)
        health = load_health(health_path)
        health["pid"] = os.getpid()
        health["started_utc"] = _iso_utc_now()
        health["started_epoch"] = time.time()
        health["paths"] = {
            "config": str(config_path),
            "state": str(state_path),
            "log": str(log_path),
        }
        health["status"] = "starting"
        save_health(health_path, health)

        if not bool(cfg.get("enabled", True)):
            logging.warning("Scanner deshabilitado en config (enabled=false).")
            save_state(state_path, state)
            health = _update_health_from_cycle(
                health=health,
                cycle_metrics=_new_cycle_metrics(),
                cycle_ok=False,
                cycle_error="Scanner deshabilitado en config (enabled=false).",
            )
            health["status"] = "disabled"
            save_health(health_path, health)
            return 0

        poll_interval = max(10, int(cfg.get("poll_interval_sec", 60)))

        while True:
            cycle_start = time.time()
            cycle_metrics = _new_cycle_metrics()
            try:
                state, cycle_metrics = run_scan_cycle(cfg, state)
                save_state(state_path, state)
                health = _update_health_from_cycle(health=health, cycle_metrics=cycle_metrics, cycle_ok=True, cycle_error="")
                save_health(health_path, health)
                logging.info(
                    "Health ciclo ok=true dur=%.1fms rss=%.1fMB records=%s err=%s alerts=%s sent=%s failed=%s",
                    float(cycle_metrics.get("duration_ms", 0.0) or 0.0),
                    float(cycle_metrics.get("rss_end_mb", 0.0) or 0.0),
                    int(cycle_metrics.get("records_total", 0) or 0),
                    int(cycle_metrics.get("records_error", 0) or 0),
                    int(cycle_metrics.get("alerts_triggered", 0) or 0),
                    int(cycle_metrics.get("alerts_sent", 0) or 0),
                    int(cycle_metrics.get("alerts_failed", 0) or 0),
                )
            except Exception as exc:
                logging.exception("Error no controlado en ciclo: %s", exc)
                cycle_metrics["duration_ms"] = round((time.time() - cycle_start) * 1000.0, 3)
                cycle_metrics.setdefault("errors", []).append(_redact_text(str(exc)))
                health = _update_health_from_cycle(
                    health=health,
                    cycle_metrics=cycle_metrics,
                    cycle_ok=False,
                    cycle_error=str(exc),
                )
                save_health(health_path, health)

            if args.once:
                break

            elapsed = time.time() - cycle_start
            sleep_sec = max(1, poll_interval - int(elapsed))
            time.sleep(sleep_sec)

        health["status"] = "stopped"
        health["last_heartbeat_utc"] = _iso_utc_now()
        save_health(health_path, health)
        logging.info("Scanner worker finalizado.")
        return 0
    finally:
        _release_single_instance_lock(LOCK_PATH)


if __name__ == "__main__":
    raise SystemExit(main())
