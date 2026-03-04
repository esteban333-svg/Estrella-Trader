from __future__ import annotations

import argparse
import json
import logging
import os
import smtplib
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
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


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = ROOT / "scanner_config.json"
DEFAULT_STATE_PATH = ROOT / "scanner_state.json"
DEFAULT_LOG_PATH = ROOT / "scanner.log"
USERS_DB_PATH = ROOT / "usuarios_db.json"
TELEGRAM_AUTO_CHAT_IDS_KEY = "telegram_auto_chat_ids"
TELEGRAM_LAST_UPDATE_ID_KEY = "telegram_last_update_id"

NY_TZ = pytz.timezone("America/New_York")

TD_INTERVAL_MAP = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1day",
}

FOREX_MAP = {
    "EUR/USD": {"ticker": "EURUSD=X", "td": "EUR/USD"},
    "GBP/USD": {"ticker": "GBPUSD=X", "td": "GBP/USD"},
    "USD/JPY": {"ticker": "JPY=X", "td": "USD/JPY"},
    "USD/CHF": {"ticker": "CHF=X", "td": "USD/CHF"},
    "AUD/USD": {"ticker": "AUDUSD=X", "td": "AUD/USD"},
    "USD/CAD": {"ticker": "CAD=X", "td": "USD/CAD"},
    "NZD/USD": {"ticker": "NZDUSD=X", "td": "NZD/USD"},
}

CRYPTO_MAP = {
    "BTC": {"ticker": "BTC-USD", "td": "BTC/USD"},
    "ETH": {"ticker": "ETH-USD", "td": "ETH/USD"},
    "SOL": {"ticker": "SOL-USD", "td": "SOL/USD"},
    "BNB": {"ticker": "BNB-USD", "td": "BNB/USD"},
    "XRP": {"ticker": "XRP-USD", "td": "XRP/USD"},
    "ADA": {"ticker": "ADA-USD", "td": "ADA/USD"},
    "DOGE": {"ticker": "DOGE-USD", "td": "DOGE/USD"},
    "WLD": {"ticker": "WLD-USD", "td": "WLD/USD"},
}


@dataclass(frozen=True)
class MarketItem:
    market: str
    label: str
    ticker: str
    td_symbol: str
    kind: str  # "forex" | "crypto"

    @property
    def state_key(self) -> str:
        return f"{self.market}|{self.label}|{self.ticker}"


def _default_config() -> Dict[str, Any]:
    return {
        "enabled": True,
        "poll_interval_sec": 60,
        "analysis_mode": "tendencial",
        "period": "5d",
        "interval": "15m",
        "cooldown_minutes": 60,
        "scan_forex": True,
        "scan_crypto": True,
        "forex_pairs": list(FOREX_MAP.keys()),
        "crypto_symbols": list(CRYPTO_MAP.keys()),
        "notification": {
            "subject_prefix": "[Estrella Trader]",
            "email": {
                "enabled": True,
                "to": [],
            },
            "telegram": {
                "enabled": True,
                "chat_ids": [],
                "parse_mode": "",
            },
            "windows": {
                "enabled": True,
            },
        },
    }


def _setup_logging(log_path: Path, debug: bool) -> None:
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def _merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


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
    return _merge_dicts(defaults, user_cfg)


def load_state(state_path: Path) -> Dict[str, Any]:
    fallback = {"symbols": {}, "free_daily_market_alerts": {}}
    state = _read_json(state_path, fallback)
    if not isinstance(state.get("symbols"), dict):
        state["symbols"] = {}
    if not isinstance(state.get("free_daily_market_alerts"), dict):
        state["free_daily_market_alerts"] = {}
    if not isinstance(state.get(TELEGRAM_AUTO_CHAT_IDS_KEY), list):
        state[TELEGRAM_AUTO_CHAT_IDS_KEY] = []
    try:
        state[TELEGRAM_LAST_UPDATE_ID_KEY] = int(state.get(TELEGRAM_LAST_UPDATE_ID_KEY, 0) or 0)
    except Exception:
        state[TELEGRAM_LAST_UPDATE_ID_KEY] = 0
    return state


def save_state(state_path: Path, state: Dict[str, Any]) -> None:
    _write_json(state_path, state)


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


def _discover_telegram_chats_from_updates(state: Dict[str, Any]) -> Tuple[List[str], str]:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    known_ids = _dedupe_chat_ids(state.get(TELEGRAM_AUTO_CHAT_IDS_KEY, []))
    if not token:
        state[TELEGRAM_AUTO_CHAT_IDS_KEY] = known_ids
        return known_ids, "TELEGRAM_BOT_TOKEN no configurado."

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
            return known_ids, f"HTTP {resp.status_code} al consultar getUpdates."
        payload = resp.json()
        if not payload.get("ok", False):
            state[TELEGRAM_AUTO_CHAT_IDS_KEY] = known_ids
            return known_ids, payload.get("description", "Error getUpdates sin descripcion.")
        updates = payload.get("result", [])
        if not isinstance(updates, list):
            updates = []
    except Exception as exc:
        state[TELEGRAM_AUTO_CHAT_IDS_KEY] = known_ids
        return known_ids, str(exc)

    max_update_id = last_update_id
    merged = list(known_ids)
    seen = set(merged)
    added = 0
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

    state[TELEGRAM_AUTO_CHAT_IDS_KEY] = merged
    state[TELEGRAM_LAST_UPDATE_ID_KEY] = max_update_id
    if added > 0:
        logging.info("Telegram: %s chat(s) nuevos detectados via getUpdates.", added)
    return merged, ""


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


def _load_user_targets(users_db_path: Path = USERS_DB_PATH) -> List[Dict[str, Any]]:
    payload = _read_json(users_db_path, {"users": []})
    users = payload.get("users") if isinstance(payload, dict) else []
    if not isinstance(users, list):
        return []

    targets: List[Dict[str, Any]] = []
    for raw in users:
        if not isinstance(raw, dict):
            continue
        user_id = str(raw.get("id", "")).strip()
        chat_id = _normalizar_telegram_chat_id(raw.get("telegram_chat_id", ""))
        if not user_id or not _chat_id_telegram_valido(chat_id):
            continue

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


def _fetch_data(item: MarketItem, period: str, interval: str) -> Tuple[pd.DataFrame | None, str, str]:
    td_key = os.getenv("TWELVE_DATA_API_KEY", "").strip()
    td_key_upper = td_key.upper()
    td_key_valid = td_key_upper not in {"", "TU_API_KEY", "YOUR_API_KEY", "CHANGE_ME"}
    td_interval = TD_INTERVAL_MAP.get(interval)
    if td_key_valid and td_interval and item.td_symbol:
        td_df, td_err = _fetch_twelvedata(item.td_symbol, td_interval, td_key)
        if td_df is not None and not td_df.empty:
            return td_df, "twelvedata", ""
        logging.warning(
            "[%s] TwelveData fallo (%s), usando yfinance fallback",
            item.state_key,
            td_err or "sin detalle",
        )

    try:
        yf_df = obtener_datos(item.ticker, periodo=period, intervalo=interval)
        return yf_df, "yfinance", ""
    except Exception as exc:
        return None, "", str(exc)


def _now_iso_utc() -> str:
    return datetime.now(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso_utc(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
    except Exception:
        return None


def _should_alert(record: Dict[str, Any], dorado_now: bool, cooldown_minutes: int) -> bool:
    if not dorado_now:
        return False

    was_dorado = bool(record.get("dorado_active", False))
    if was_dorado:
        return False

    last_alert_iso = str(record.get("last_alert_utc", "")).strip()
    last_alert_dt = _parse_iso_utc(last_alert_iso) if last_alert_iso else None
    if last_alert_dt is None:
        return True

    delta_sec = (datetime.now(pytz.UTC) - last_alert_dt).total_seconds()
    return delta_sec >= max(1, cooldown_minutes) * 60


def _build_watchlist(cfg: Dict[str, Any]) -> List[MarketItem]:
    watchlist: List[MarketItem] = []

    if bool(cfg.get("scan_forex", True)):
        selected_pairs = cfg.get("forex_pairs", list(FOREX_MAP.keys()))
        for pair in selected_pairs:
            row = FOREX_MAP.get(pair)
            if not row:
                logging.warning("Par forex desconocido en config: %s", pair)
                continue
            watchlist.append(
                MarketItem(
                    market="Forex",
                    label=pair,
                    ticker=row["ticker"],
                    td_symbol=row.get("td", ""),
                    kind="forex",
                )
            )

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
                )
            )

    return watchlist


def _build_alert_payload(cfg: Dict[str, Any], item: MarketItem, estado: Dict[str, Any], source: str) -> Tuple[str, str]:
    dorado = estado.get("dorado_v13") or {}
    score = dorado.get("micro_score")
    umbral = dorado.get("umbral")
    rr = dorado.get("rr_estimado")
    riesgo = estado.get("riesgo", "")
    decision = estado.get("decision", "")
    resumen = estado.get("mensaje", dorado.get("resumen", "Dorado activado."))
    direction = estado.get("direccion_v13", "")
    modo = str(estado.get("modo_alerta", "Tendencial")).strip() or "Tendencial"
    modo_lower = modo.lower()
    if "estructural" in modo_lower:
        modo_label = "Estructural"
    elif "tendencial" in modo_lower:
        modo_label = "Tendencial"
    else:
        modo_label = modo
    temporalidad = str(estado.get("temporalidad_alerta", "")).strip()
    if not temporalidad:
        temporalidad = "1D + 4H" if "estructural" in modo.lower() else str(cfg.get("interval", "15m"))

    prefix = cfg.get("notification", {}).get("subject_prefix", "[Estrella Trader]")
    subject = f"{prefix} DORADO ACTIVADO | {item.market} {item.label} | {item.ticker}"

    body = (
        f"Dorado activado en {item.market} ({item.label})\n"
        f"Ticker: {item.ticker}\n"
        f"Modo: {modo_label}\n"
        f"Timeframe: {temporalidad}\n"
        f"Direccion: {direction}\n"
        f"Decision: {decision}\n"
        f"Riesgo: {riesgo}\n"
        f"Micro score: {score}\n"
        f"Umbral: {umbral}\n"
        f"RR estimado: {rr}\n"
        f"Fuente de datos: {source}\n"
        f"Hora UTC: {_now_iso_utc()}\n\n"
        f"Resumen:\n{resumen}\n"
    )
    return subject, body


def _analysis_mode(cfg: Dict[str, Any]) -> str:
    raw = str(cfg.get("analysis_mode", "tendencial") or "").strip().lower()
    if raw in {"estructural", "structural", "1d+4h", "1d_4h"}:
        return "estructural"
    return "tendencial"


def _compute_estado(item: MarketItem, cfg: Dict[str, Any]) -> Tuple[Dict[str, Any] | None, str, str]:
    mode = _analysis_mode(cfg)
    if mode == "estructural":
        df_1d, source_1d, err_1d = _fetch_data(item, period="3y", interval="1d")
        if df_1d is None or df_1d.empty:
            return None, "", f"1D: {err_1d or 'sin datos'}"

        df_4h, source_4h, err_4h = _fetch_data(item, period="12mo", interval="4h")
        if df_4h is None or df_4h.empty:
            return None, "", f"4H: {err_4h or 'sin datos'}"

        try:
            df_1d_ind = calcular_indicadores(df_1d)
            df_4h_ind = calcular_indicadores(df_4h)
            estado = construir_estado_final_estructural(df_1d_ind, df_4h_ind, impacto_memoria=0)
        except Exception as exc:
            return None, "", f"Error estructural: {exc}"

        if not isinstance(estado, dict):
            return None, "", "Estado estructural invalido."
        estado["modo_alerta"] = "Estructural (1D+4H)"
        estado["temporalidad_alerta"] = "1D + 4H"
        return estado, f"1D:{source_1d} | 4H:{source_4h}", ""

    interval = str(cfg.get("interval", "15m"))
    period = str(cfg.get("period", "5d"))
    df, source, fetch_err = _fetch_data(item, period=period, interval=interval)
    if df is None or df.empty:
        return None, "", fetch_err or "No se pudieron descargar velas."

    try:
        df_ind = calcular_indicadores(df)
        estado = construir_estado_final(df_ind, impacto_memoria=0)
    except Exception as exc:
        return None, "", f"Error calculando estado: {exc}"

    if not isinstance(estado, dict):
        return None, "", "Estado tendencial invalido."
    estado["modo_alerta"] = "Tendencial"
    estado["temporalidad_alerta"] = interval
    return estado, source, ""


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
        return False, str(exc)


def _send_telegram_alert(cfg: Dict[str, Any], subject: str, body: str, chat_ids_override: List[str] | None = None) -> Tuple[bool, str]:
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

    errors = []
    sent_any = False
    for chat_id in chat_ids:
        try:
            payload: Dict[str, Any] = {"chat_id": chat_id, "text": message}
            if parse_mode:
                payload["parse_mode"] = parse_mode
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=payload,
                timeout=20,
            )
            if resp.status_code >= 400:
                errors.append(f"{chat_id}: HTTP {resp.status_code}")
                continue
            data = resp.json()
            if not data.get("ok", False):
                errors.append(f"{chat_id}: {data.get('description', 'Error Telegram sin descripcion')}")
                continue
            sent_any = True
        except Exception as exc:
            errors.append(f"{chat_id}: {exc}")

    if sent_any:
        return True, ""
    return False, "; ".join(errors) if errors else "No se pudo enviar a Telegram."


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
            return False, stderr or stdout or f"powershell returncode={proc.returncode}"
        return True, ""
    except Exception as exc:
        return False, str(exc)


def _channel_enabled(cfg: Dict[str, Any], channel: str, default: bool = True) -> bool:
    notif = cfg.get("notification", {}) if isinstance(cfg.get("notification", {}), dict) else {}
    ch = notif.get(channel, {})
    if not isinstance(ch, dict):
        return default
    raw = ch.get("enabled", default)
    return bool(raw)


def run_scan_cycle(cfg: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    cooldown = int(cfg.get("cooldown_minutes", 60))
    watchlist = _build_watchlist(cfg)

    if not watchlist:
        logging.warning("Watchlist vacia. Revisa scanner_config.json")
        return state

    symbols_state: Dict[str, Any] = state.setdefault("symbols", {})
    configured_telegram_chat_ids = _dedupe_chat_ids(_parse_telegram_chat_ids(cfg))
    auto_telegram_chat_ids: List[str] = []
    if _channel_enabled(cfg, "telegram", default=True):
        auto_telegram_chat_ids, discover_err = _discover_telegram_chats_from_updates(state)
        if discover_err:
            logging.warning("Telegram getUpdates: %s", discover_err)
    telegram_broadcast_ids = _dedupe_chat_ids(configured_telegram_chat_ids + auto_telegram_chat_ids)

    for item in watchlist:
        record = symbols_state.get(item.state_key, {})
        record["last_checked_utc"] = _now_iso_utc()

        if not _market_open(item.kind):
            record["market_open"] = False
            record["last_error"] = ""
            symbols_state[item.state_key] = record
            logging.debug("[%s] Mercado cerrado, se omite ciclo.", item.state_key)
            continue

        record["market_open"] = True

        estado, source, compute_err = _compute_estado(item, cfg)
        if estado is None:
            record["last_error"] = compute_err or "No se pudo calcular estado."
            symbols_state[item.state_key] = record
            logging.warning("[%s] Error de datos/estado: %s", item.state_key, record["last_error"])
            continue

        dorado = estado.get("dorado_v13")
        dorado_now = bool(dorado)

        if _should_alert(record, dorado_now, cooldown_minutes=cooldown):
            subject, body = _build_alert_payload(cfg, item, estado, source)
            notify_status: Dict[str, Any] = {}
            sent_any = False
            errors: List[str] = []
            enabled_any = False
            user_targets = _load_user_targets()
            market_key = f"{item.market}|{item.label}"

            if user_targets:
                user_results: List[Dict[str, Any]] = []
                premium_users_alerted = False
                eligible_users = 0

                telegram_enabled = _channel_enabled(cfg, "telegram", default=True)
                email_enabled = _channel_enabled(cfg, "email", default=True)
                windows_enabled = _channel_enabled(cfg, "windows", default=True)

                if telegram_enabled:
                    enabled_any = True
                if email_enabled:
                    enabled_any = True
                if windows_enabled:
                    enabled_any = True

                for target in user_targets:
                    user_id = target["id"]
                    chat_id = target["chat_id"]
                    email = target.get("email", "")
                    is_premium = bool(target.get("es_premium", False))

                    if not is_premium and not _free_user_can_receive_market_alert(state, user_id, market_key):
                        continue

                    eligible_users += 1
                    user_notified = False
                    user_errors: List[str] = []

                    if telegram_enabled:
                        sent_ok, sent_err = _send_telegram_alert(cfg, subject, body, chat_ids_override=[chat_id])
                        if sent_ok:
                            user_notified = True
                            sent_any = True
                        else:
                            user_errors.append(f"telegram: {sent_err}")

                    if is_premium and email_enabled and email:
                        sent_ok, sent_err = _send_email_alert(
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
                    sent_ok, sent_err = _send_telegram_alert(
                        cfg,
                        subject,
                        body,
                        chat_ids_override=broadcast_extra_chat_ids,
                    )
                    notify_status["telegram_broadcast"] = {
                        "ok": sent_ok,
                        "error": sent_err,
                        "sent_to": len(broadcast_extra_chat_ids),
                    }
                    if sent_ok:
                        sent_any = True
                    else:
                        errors.append(f"telegram_broadcast: {sent_err}")

                if eligible_users == 0:
                    errors.append("Sin usuarios elegibles con Telegram para esta alerta.")

                if windows_enabled:
                    if premium_users_alerted:
                        sent_ok, sent_err = _send_windows_toast(subject, body)
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
                    sent_ok, sent_err = _send_email_alert(cfg, item, subject, body)
                    notify_status["email"] = {"ok": sent_ok, "error": sent_err}
                    if sent_ok:
                        sent_any = True
                    else:
                        errors.append(f"email: {sent_err}")

                if _channel_enabled(cfg, "telegram", default=True):
                    enabled_any = True
                    sent_ok, sent_err = _send_telegram_alert(
                        cfg,
                        subject,
                        body,
                        chat_ids_override=telegram_broadcast_ids or None,
                    )
                    notify_status["telegram"] = {"ok": sent_ok, "error": sent_err}
                    if sent_ok:
                        sent_any = True
                    else:
                        errors.append(f"telegram: {sent_err}")

                if _channel_enabled(cfg, "windows", default=True):
                    enabled_any = True
                    sent_ok, sent_err = _send_windows_toast(subject, body)
                    notify_status["windows"] = {"ok": sent_ok, "error": sent_err}
                    if sent_ok:
                        sent_any = True
                    else:
                        errors.append(f"windows: {sent_err}")

                if not enabled_any:
                    errors.append("No hay canales habilitados en notification.")

            record["last_notify"] = notify_status
            if sent_any:
                record["last_alert_utc"] = _now_iso_utc()
                logging.info("[%s] Alerta enviada (multicanal).", item.state_key)
            else:
                record["last_error"] = "; ".join(errors)
                logging.error("[%s] Fallo envio alerta: %s", item.state_key, record["last_error"])

        record["dorado_active"] = dorado_now
        record["last_source"] = source
        record["decision"] = estado.get("decision", "")
        record["riesgo"] = estado.get("riesgo", "")
        record["direccion"] = estado.get("direccion_v13", "")
        if not record.get("last_error"):
            record["last_error"] = ""
        symbols_state[item.state_key] = record

    return state


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Estrella Trader market scanner worker")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Ruta al scanner_config.json")
    parser.add_argument("--state", default=str(DEFAULT_STATE_PATH), help="Ruta al scanner_state.json")
    parser.add_argument("--log", default=str(DEFAULT_LOG_PATH), help="Ruta al scanner.log")
    parser.add_argument("--once", action="store_true", help="Ejecuta un solo ciclo y termina.")
    parser.add_argument("--debug", action="store_true", help="Activa logs DEBUG.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config).resolve()
    state_path = Path(args.state).resolve()
    log_path = Path(args.log).resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    _setup_logging(log_path, debug=bool(args.debug))
    logging.info("Scanner worker iniciado.")

    cfg = load_config(config_path)
    state = load_state(state_path)

    if not bool(cfg.get("enabled", True)):
        logging.warning("Scanner deshabilitado en config (enabled=false).")
        save_state(state_path, state)
        return 0

    poll_interval = max(10, int(cfg.get("poll_interval_sec", 60)))

    while True:
        cycle_start = time.time()
        try:
            state = run_scan_cycle(cfg, state)
            save_state(state_path, state)
        except Exception as exc:
            logging.exception("Error no controlado en ciclo: %s", exc)

        if args.once:
            break

        elapsed = time.time() - cycle_start
        sleep_sec = max(1, poll_interval - int(elapsed))
        time.sleep(sleep_sec)

    logging.info("Scanner worker finalizado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
