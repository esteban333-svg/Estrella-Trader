# -----------------------
# IMPORTS DEL PROYECTO DEJAR ARRIBA
# -----------------------
from analysis import obtener_precio_live
import streamlit as st
from estrella_ui import render_estado_estrella
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pytz
import requests
import pandas as pd
from analysis import interpretar_bollinger
import time
import math
import os
import random
import base64
import json
import hashlib
import smtplib
from email.message import EmailMessage
from uuid import uuid4
from datetime import datetime, timedelta
from analysis import (
    obtener_datos,
    calcular_indicadores,
    contexto_mercado,
    interpretar_rsi,
    aplicar_ensenar,
    construir_estado_final,
    construir_estado_final_estructural,
    resumen_estado_humano
)

from sessions import (
    sesion_actual,
    calidad_horario,
    explicacion_horario
)
from analysis import advertencia_por_memoria
from memoria import influencia_de_memoria, recuerdos_relevantes, clasificar_errores
from live_binance import BinanceLiveStore, fetch_klines, start_stream, WEBSOCKETS_AVAILABLE
try:
    from streamlit_autorefresh import st_autorefresh
    _HAS_AUTOREFRESH = True
except Exception:
    st_autorefresh = None
    _HAS_AUTOREFRESH = False

# -----------------------
# CONFIGURACION DE LA APP (SIEMPRE PRIMERO)
# -----------------------
st.set_page_config(
    page_title="Estrella Trader V1" ,
    layout="wide"
)


# ========= [V1.2-B THEME - INICIO] =========

COL_BG = "#0F1117"
COL_CARD = "#161A23"
COL_TEXT = "#E6E8EE"
COL_TEXT_2 = "#9AA1B2"
COL_MUTE = "#6B7280"

COL_BLUE = "#4A90E2"
COL_GOLD = "#F5C26B"
COL_RED = "#E26D5A"

st.markdown(f"""
<style>
/* Fondo general */
.stApp {{
  background-color: {COL_BG};
  color: {COL_TEXT};
}}

/* Texto secundario */
.small-muted {{
  color: {COL_TEXT_2};
  font-size: 0.92rem;
}}

/* Card base */
.et-card {{
  background: {COL_CARD};
  border: 1px solid rgba(255,255,255,0.06);
  border-radius: 16px;
  padding: 14px 16px;
  margin: 10px 0 14px 0;
}}

/* Título de bloque */
.et-title {{
  font-weight: 700;
  font-size: 1.02rem;
  margin-bottom: 8px;
}}

/* Mensaje Estrella con borde por esfera */
.et-star-msg {{
  background: {COL_CARD};
  border-radius: 16px;
  padding: 10px 14px;
  margin: 8px 0 10px 0;
  border-left: 4px solid var(--accent);
  border-top: 1px solid rgba(255,255,255,0.06);
  border-right: 1px solid rgba(255,255,255,0.06);
  border-bottom: 1px solid rgba(255,255,255,0.06);
  display: inline-block;
  max-width: 680px;
}}
.et-star-msg p {{
  margin: 0 0 6px 0;
}}
.et-star-msg ul {{
  margin: 6px 0 6px 18px;
  padding: 0;
}}
.et-star-msg li {{
  margin: 0 0 4px 0;
}}

/* Decisión (una palabra grande) */
.et-decision {{
  font-weight: 800;
  font-size: 1.55rem;
  line-height: 1.1;
  margin: 6px 0 2px 0;
}}

/* ===== Panel principal flotante ===== */
.et-float-panel{{
  position: fixed;
  top: 86px;           /* ajusta si tu header ocupa m?s/menos */
  right: 22px;
  width: 240px;
  z-index: 9999;
  box-shadow: 0 18px 40px rgba(0,0,0,0.35);
}}

.et-float-panel .et-card{{
  padding: 12px 14px;
}}

.pos-tr {{
  top: 86px;
  right: 22px;
  left: auto;
  bottom: auto;
}}

.pos-tl {{
  top: 86px;
  left: 22px;
  right: auto;
  bottom: auto;
}}

.pos-br {{
  top: auto;
  right: 22px;
  left: auto;
  bottom: 18px;
}}

.pos-bl {{
  top: auto;
  left: 22px;
  right: auto;
  bottom: 18px;
}}

/* Para que no se coma el contenido: reservamos espacio a la derecha */
.et-right-spacer{{
  height: 1px;
}}

/* Responsivo: en pantallas peque?as lo pegamos abajo para no tapar todo */
@media (max-width: 900px){{
  .et-float-panel{{
    position: fixed;
    top: auto;
    bottom: 18px;
    left: 18px;
    right: 18px;
    width: auto;
    
  }}
}}

/* ===== Trazabilidad / Recuerdos ===== */
.et-row {{
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
  margin: 6px 0 2px 0;
}}

.et-pill {{
  display: inline-block;
  padding: 4px 10px;
  border-radius: 999px;
  background: rgba(255,255,255,0.06);
  border: 1px solid rgba(255,255,255,0.08);
  font-size: 0.85rem;
  color: #E6E8EE;
}}

.et-kv {{
  color: #9AA1B2;
  font-size: 0.9rem;
  margin-top: 6px;
}}

.et-kv b{{
  color: #E6E8EE;
  font-weight: 700;
}}

.et-snippet {{
  margin-top: 10px;
  color: #E6E8EE;
  white-space: pre-line;
}}

/* Logo estrella girando */
.et-rotating-star {{
  animation: et-spin 12s linear infinite;
  transform-origin: 50% 50%;
}}
@keyframes et-spin {{
  from {{ transform: rotate(0deg); }}
  to {{ transform: rotate(360deg); }}
}}
@media (prefers-reduced-motion: reduce) {{
  .et-rotating-star {{
    animation: none;
  }}
}}

.et-divider {{
  height: 1px;
  background: rgba(255,255,255,0.06);
  margin: 10px 0;
}}

.et-mini {{
  font-size: 0.86rem;
  color: #9AA1B2;
}}

/* ===== Guia rapida inicial (modal) ===== */
.et-guide-backdrop {{
  position: fixed;
  inset: 0;
  background: rgba(8, 10, 16, 0.58);
  z-index: 10018;
}}

.st-key-onboarding_modal {{
  position: fixed;
  top: 8vh;
  left: 75%;
  transform: translateX(-50%);
  width: min(780px, calc(100vw - 36px));
  z-index: 10019;
}}

.st-key-onboarding_modal > div {{
  background: #111827;
  border: 1px solid rgba(255,255,255,0.12);
  border-radius: 16px;
  padding: 16px 18px;
  box-shadow: 0 22px 48px rgba(0,0,0,0.5);
  max-height: 80vh;
  overflow: auto;
}}

.st-key-onboarding_modal .stButton > button {{
  width: 100%;
  border-radius: 10px;
  border: 1px solid rgba(255,255,255,0.18);
  background: rgba(255,255,255,0.1);
  color: #E6E8EE;
  font-weight: 700;
}}

@media (max-width: 900px) {{
  .st-key-onboarding_modal {{
    top: 4vh;
    left: 50%;
    transform: translateX(-50%);
    width: calc(100vw - 24px);
  }}
}}

/* ===== Notas (textarea) ===== */
div[data-testid="stTextArea"] textarea {{
  background-color: {COL_CARD};
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 12px;
  color: {COL_TEXT};
}}
</style>
""", unsafe_allow_html=True)

# ====== BOTONES ENSEÑAR (COLORES) ======
st.markdown("""
<style>
.et-btn-ensenar .stButton>button {
  width: 44px;
  height: 44px;
  border-radius: 50%;
  font-weight: 700;
  padding: 0;
  border: 1px solid rgba(255,255,255,0.12);
  box-shadow: inset 0 1px 2px rgba(255,255,255,0.35), 0 6px 14px rgba(0,0,0,0.25);
}
.et-btn-col {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 10px;
  margin-top: 10px;
}
.et-btn-dorado .stButton>button {
  background: radial-gradient(circle at 30% 30%, #FFE5A8 0%, #F5C26B 55%, #D19A3E 100%);
  color: #1B1B1B;
}
.et-btn-azul .stButton>button {
  background: radial-gradient(circle at 30% 30%, #9CC6FF 0%, #4A90E2 55%, #2F6FBF 100%);
  color: #0B1620;
}
.et-btn-rojo .stButton>button {
  background: radial-gradient(circle at 30% 30%, #FFB0A1 0%, #E26D5A 55%, #C55341 100%);
  color: #1A0F0C;
}
.et-filter .stButton>button {
  width: 40px;
  height: 40px;
  border-radius: 50%;
  padding: 0;
  border: 1px solid rgba(255,255,255,0.12);
  box-shadow: inset 0 1px 2px rgba(255,255,255,0.35), 0 6px 14px rgba(0,0,0,0.25);
}
.et-filter {
  display: flex;
  justify-content: flex-end;
}
.et-filter-dorado .stButton>button {
  background: radial-gradient(circle at 30% 30%, #FFE5A8 0%, #F5C26B 55%, #D19A3E 100%);
  color: #1B1B1B;
}
.et-filter-azul .stButton>button {
  background: radial-gradient(circle at 30% 30%, #9CC6FF 0%, #4A90E2 55%, #2F6FBF 100%);
  color: #0B1620;
}
.et-filter-rojo .stButton>button {
  background: radial-gradient(circle at 30% 30%, #FFB0A1 0%, #E26D5A 55%, #C55341 100%);
  color: #1A0F0C;
}
</style>
""", unsafe_allow_html=True)


# ========= [V1.2-B THEME - FIN] =========
# ========= [V1.2-B HELPERS - INICIO] =========
def color_por_decision(decision: str) -> str:
    d = (decision or "").upper()
    if d == "OBSERVAR":
        return COL_BLUE
    if d == "NO OPERAR":
        return COL_RED
    if d == "OPERAR":
        return COL_GOLD
    if d == "MERCADO CERRADO":
        return COL_TEXT
    return COL_TEXT_2


def color_por_esfera(esfera: str) -> str:
    s = (esfera or "").lower()
    if "azul" in s:
        return COL_BLUE
    if "roja" in s or "rojo" in s:
        return COL_RED
    if "dorada" in s or "dorado" in s or "amarill" in s:
        return COL_GOLD
    return COL_TEXT_2


def normalizar_texto_ui(texto):
    if not isinstance(texto, str) or not texto:
        return texto
    s = texto
    # Intento 1: latin1 -> utf8 (caso más común de mojibake)
    try:
        s = s.encode("latin1").decode("utf-8")
    except Exception:
        pass

    # Reemplazos explícitos para residuos frecuentes
    fixes = {
        "ðŸ§­": "🧭",
        "GuÃa": "Guía",
        "Ã¡": "á",
        "Ã©": "é",
        "Ã­": "í",
        "Ã³": "ó",
        "Ãº": "ú",
        "Ã": "Á",
        "Ã‰": "É",
        "Ã": "Í",
        "Ã“": "Ó",
        "Ãš": "Ú",
        "Ã±": "ñ",
        "Ã‘": "Ñ",
        "â€”": "—",
        "â€“": "–",
        "â€œ": "\"",
        "â€": "\"",
        "â€™": "'",
    }
    for bad, good in fixes.items():
        s = s.replace(bad, good)

    return s


def cargar_guia_rapida_markdown() -> str:
    ruta = os.path.join(os.path.dirname(__file__), "README_RAPIDO.md")
    try:
        with open(ruta, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return (
            "# Guia Rapida\n\n"
            "No se pudo leer README_RAPIDO.md.\n\n"
            "- Revisa Panel principal.\n"
            "- Usa Dorado/Rojo como filtro de ejecucion.\n"
            "- Si hay duda, observar."
        )


def rerun_app():
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()


USERS_DB_PATH = os.path.join(os.path.dirname(__file__), "usuarios_db.json")


def _utc_now():
    return datetime.now(pytz.UTC)


def _iso_utc(dt_obj: datetime) -> str:
    return dt_obj.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso_utc(value: str):
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
    except Exception:
        return None


def _normalizar_email(email: str) -> str:
    return (email or "").strip().lower()


def _es_gmail(email: str) -> bool:
    email = _normalizar_email(email)
    if "@" not in email:
        return False
    return email.endswith("@gmail.com")


def _enviar_correo_bienvenida(username: str, email_to: str):
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port_raw = os.getenv("SMTP_PORT", "587")
    smtp_user = os.getenv("SMTP_USER", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "").strip()
    smtp_from = os.getenv("SMTP_FROM", smtp_user).strip()
    app_name = os.getenv("APP_DISPLAY_NAME", "Estrella Trader V1")

    try:
        smtp_port = int(smtp_port_raw)
    except Exception:
        smtp_port = 587

    if not smtp_user or not smtp_password or not smtp_from:
        return False, "SMTP no configurado (SMTP_USER/SMTP_PASSWORD/SMTP_FROM)."

    asunto = f"Bienvenido a {app_name}"
    cuerpo = (
        f"Hola {username},\n\n"
        "Tu cuenta fue creada correctamente.\n"
        "Ya puedes iniciar sesion y usar la app.\n\n"
        "Premium te da acceso a herramientas avanzadas dentro de la Estrella.\n"
        "Cuando quieras, activalo desde el panel de Cuenta.\n\n"
        "Gracias por usar Estrella Trader."
    )

    msg = EmailMessage()
    msg["Subject"] = asunto
    msg["From"] = smtp_from
    msg["To"] = email_to
    msg.set_content(cuerpo)

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


def _hash_password(password: str) -> str:
    return hashlib.sha256((password or "").encode("utf-8")).hexdigest()


def _cargar_usuarios_db() -> dict:
    try:
        with open(USERS_DB_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict) and isinstance(payload.get("users"), list):
            return payload
    except Exception:
        pass
    return {"users": []}


def _guardar_usuarios_db(payload: dict):
    with open(USERS_DB_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _premium_activo(record: dict) -> bool:
    if not bool(record.get("es_premium", False)):
        return False
    until = record.get("premium_until")
    if not until:
        return True
    until_dt = _parse_iso_utc(until)
    if until_dt is None:
        return bool(record.get("es_premium", False))
    return _utc_now() <= until_dt


def _usuario_publico(record: dict) -> dict:
    activo = _premium_activo(record)
    return {
        "id": record.get("id"),
        "username": record.get("username", ""),
        "email": record.get("email", ""),
        "es_premium": activo,
        "premium_until": record.get("premium_until"),
        "autenticado": True,
    }


def _usuario_invitado() -> dict:
    return {
        "id": "guest",
        "username": "Invitado",
        "email": "",
        "es_premium": False,
        "premium_until": None,
        "autenticado": False,
    }


def registrar_usuario(username: str, email: str, password: str):
    username = (username or "").strip()
    email = _normalizar_email(email)
    password = password or ""

    if len(username) < 3:
        return False, "El nombre de usuario debe tener al menos 3 caracteres.", None
    if not _es_gmail(email):
        return False, "Usa un correo Gmail valido (ejemplo@gmail.com).", None
    if len(password) < 6:
        return False, "La contraseña debe tener al menos 6 caracteres.", None

    db = _cargar_usuarios_db()
    if any(_normalizar_email(u.get("email")) == email for u in db["users"]):
        return False, "Ya existe una cuenta con ese correo.", None

    now_iso = _iso_utc(_utc_now())
    record = {
        "id": str(uuid4()),
        "username": username,
        "email": email,
        "password_hash": _hash_password(password),
        "es_premium": False,
        "premium_until": None,
        "created_at": now_iso,
        "updated_at": now_iso,
    }
    db["users"].append(record)
    _guardar_usuarios_db(db)
    ok_mail, mail_err = _enviar_correo_bienvenida(username, email)
    if ok_mail:
        msg = "Cuenta creada correctamente. Te enviamos un correo de bienvenida."
    else:
        msg = (
            "Cuenta creada correctamente. "
            f"No se pudo enviar el correo de bienvenida: {mail_err}"
        )
    return True, msg, _usuario_publico(record)


def autenticar_usuario(email: str, password: str):
    email = _normalizar_email(email)
    if not _es_gmail(email):
        return False, "Ingresa un correo Gmail valido.", None
    password_hash = _hash_password(password or "")

    db = _cargar_usuarios_db()
    changed = False
    for u in db["users"]:
        if _normalizar_email(u.get("email")) != email:
            continue
        if u.get("password_hash") != password_hash:
            return False, "Credenciales inválidas.", None

        if u.get("es_premium", False) and not _premium_activo(u):
            u["es_premium"] = False
            u["updated_at"] = _iso_utc(_utc_now())
            changed = True

        if changed:
            _guardar_usuarios_db(db)
        return True, "Sesión iniciada.", _usuario_publico(u)

    return False, "Credenciales inválidas.", None


def recargar_usuario(user_id: str):
    db = _cargar_usuarios_db()
    changed = False
    for u in db["users"]:
        if u.get("id") != user_id:
            continue
        if u.get("es_premium", False) and not _premium_activo(u):
            u["es_premium"] = False
            u["updated_at"] = _iso_utc(_utc_now())
            changed = True
        if changed:
            _guardar_usuarios_db(db)
        return _usuario_publico(u)
    return _usuario_invitado()


def comprar_premium_usuario(user_id: str, plan_dias: int):
    db = _cargar_usuarios_db()
    for u in db["users"]:
        if u.get("id") != user_id:
            continue
        now = _utc_now()
        try:
            dias = int(plan_dias)
        except Exception:
            return False, "Plan inválido.", None
        if dias <= 0:
            return False, "Plan inválido.", None

        # Si el usuario ya tiene premium vigente, extiende desde su vencimiento actual.
        base_dt = now
        until_actual = _parse_iso_utc(u.get("premium_until")) if u.get("premium_until") else None
        if bool(u.get("es_premium", False)) and until_actual and until_actual > now:
            base_dt = until_actual

        u["es_premium"] = True
        u["premium_until"] = _iso_utc(base_dt + timedelta(days=dias))
        u["last_purchase"] = {
            "fecha": _iso_utc(now),
            "dias": dias,
        }
        u["updated_at"] = _iso_utc(now)
        _guardar_usuarios_db(db)
        return True, "Premium activado correctamente.", _usuario_publico(u)
    return False, "No se encontró el usuario para activar Premium.", None


def set_premium_usuario(user_id: str, enabled: bool, plan_dias: int = 30):
    db = _cargar_usuarios_db()
    for u in db["users"]:
        if u.get("id") != user_id:
            continue
        now = _utc_now()
        if enabled:
            try:
                dias = int(plan_dias)
            except Exception:
                dias = 30
            if dias <= 0:
                dias = 30
            u["es_premium"] = True
            u["premium_until"] = _iso_utc(now + timedelta(days=dias))
            msg = "Premium activado (modo temporal)."
        else:
            u["es_premium"] = False
            u["premium_until"] = None
            msg = "Premium desactivado."
        u["updated_at"] = _iso_utc(now)
        _guardar_usuarios_db(db)
        return True, msg, _usuario_publico(u)
    return False, "No se encontró el usuario.", None


def ajustar_timezone(df, zona_ui):
    tz_map = {
        "UTC": "UTC",
        "Bogotá": "America/Bogota",
        "New York": "America/New_York",
        "Londres": "Europe/London",
        "Madrid": "Europe/Madrid",
    }
    tz_name = tz_map.get(zona_ui, "UTC")
    tz = pytz.timezone(tz_name)

    if df is None or df.empty:
        return df

    idx = df.index
    if getattr(idx, "tz", None) is None:
        idx = idx.tz_localize("UTC")
    else:
        idx = idx.tz_convert("UTC")

    df = df.copy()
    df.index = idx.tz_convert(tz)
    return df


TD_TZ_MAP = {
    "UTC": "UTC",
    "Bogotá": "America/Bogota",
    "New York": "America/New_York",
    "Londres": "Europe/London",
    "Madrid": "Europe/Madrid",
}

TD_INTERVAL_MAP = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1day",
}

TD_SYMBOLS = {
    "Oro (XAU/USD)": "XAU/USD",
    "NASDAQ 100": "NDX",
    "S&P 500": "SPX",
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
    "BTC": {"ticker": "BTC-USD", "binance": "BTCUSDT", "td": "BTC/USD"},
    "ETH": {"ticker": "ETH-USD", "binance": "ETHUSDT", "td": "ETH/USD"},
    "SOL": {"ticker": "SOL-USD", "binance": "SOLUSDT", "td": "SOL/USD"},
    "BNB": {"ticker": "BNB-USD", "binance": "BNBUSDT", "td": "BNB/USD"},
    "XRP": {"ticker": "XRP-USD", "binance": "XRPUSDT", "td": "XRP/USD"},
    "ADA": {"ticker": "ADA-USD", "binance": "ADAUSDT", "td": "ADA/USD"},
    "DOGE": {"ticker": "DOGE-USD", "binance": "DOGEUSDT", "td": "DOGE/USD"},
    "WLD": {"ticker": "WLD-USD", "binance": "WLDUSDT", "td": "WLD/USD"},
}


def obtener_datos_twelvedata(symbol, interval, outputsize, api_key):
    try:
        params = {
            "symbol": symbol,
            "interval": interval,
            "outputsize": outputsize,
            "format": "JSON",
            "apikey": api_key,
        }
        resp = requests.get("https://api.twelvedata.com/time_series", params=params, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        values = payload.get("values")
        if not values:
            return None
        df = pd.DataFrame(values)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime")
        df = df.set_index("datetime")
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.rename(columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        })
        return df
    except Exception:
        return None


def obtener_precio_live_twelvedata(symbol, api_key):
    try:
        params = {"symbol": symbol, "apikey": api_key, "format": "JSON"}
        resp = requests.get("https://api.twelvedata.com/price", params=params, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        price = payload.get("price")
        return float(price) if price is not None else None
    except Exception:
        return None


def mercado_abierto_ahora(mercado: str) -> bool:
    """
    Regla simple 24/5 para mercados no-crypto usando horario New York.
    Cierre semanal: viernes 17:00 NY -> domingo 17:00 NY.
    """
    if mercado == "Crypto":
        return True

    ny_now = datetime.now(pytz.timezone("America/New_York"))
    wd = ny_now.weekday()  # 0=lunes ... 6=domingo
    hhmm = ny_now.hour * 60 + ny_now.minute
    cierre = 17 * 60

    if wd == 5:  # sábado
        return False
    if wd == 6:  # domingo
        return hhmm >= cierre
    if wd == 4:  # viernes
        return hhmm < cierre
    return True


@st.cache_data(ttl=5, show_spinner=False)
def obtener_datos_procesados_cache(
    ticker: str,
    periodo: str,
    intervalo: str,
    td_symbol_value: str | None,
    td_interval_value: str | None,
    td_api_key_value: str | None,
    zona_ui: str,
):
    datos_local = None

    # Prioriza TwelveData cuando existe API key (especialmente útil para FX).
    if td_api_key_value and td_symbol_value and td_interval_value:
        td_df = obtener_datos_twelvedata(td_symbol_value, td_interval_value, 500, td_api_key_value)
        if td_df is not None and not td_df.empty:
            datos_local = td_df

    if datos_local is None:
        try:
            datos_local = obtener_datos(ticker=ticker, periodo=periodo, intervalo=intervalo)
        except Exception:
            datos_local = pd.DataFrame()

    # Fallback para 4H cuando el proveedor no lo soporta nativamente:
    # reconstruye 4H desde 1H para mantener continuidad en la lectura.
    if (datos_local is None or datos_local.empty) and intervalo == "4h":
        try:
            datos_1h = obtener_datos(ticker=ticker, periodo=periodo, intervalo="1h")
            if datos_1h is not None and not datos_1h.empty:
                datos_1h = datos_1h.sort_index().copy()
                agg = {
                    "Open": "first",
                    "High": "max",
                    "Low": "min",
                    "Close": "last",
                }
                if "Volume" in datos_1h.columns:
                    agg["Volume"] = "sum"
                datos_local = datos_1h.resample("4h").agg(agg)
                datos_local = datos_local.dropna(subset=["Open", "High", "Low", "Close"])
                if "Volume" not in datos_local.columns:
                    datos_local["Volume"] = 0.0
        except Exception:
            datos_local = pd.DataFrame()

    if datos_local is None or datos_local.empty:
        raise ValueError("No se pudieron obtener datos del mercado")

    datos_local = calcular_indicadores(datos_local)
    datos_local["dist_ema200"] = ((datos_local["Close"] - datos_local["EMA_200"]) / datos_local["EMA_200"]) * 100
    return ajustar_timezone(datos_local, zona_ui)


@st.cache_data(ttl=3, show_spinner=False)
def construir_estado_final_cache(datos_df):
    return construir_estado_final(datos_df, impacto_memoria=0)


@st.cache_data(ttl=3, show_spinner=False)
def construir_estado_estructural_cache(datos_1d_df, datos_4h_df):
    return construir_estado_final_estructural(datos_1d_df, datos_4h_df, impacto_memoria=0)


# ========= [V1.2-B HELPERS - FIN] =========

if "memoria_estrella" not in st.session_state:
    st.session_state.memoria_estrella = []


def filtrar_por_esfera(recuerdos, esfera):
    return [r for r in recuerdos if r.get("esfera") == esfera]


# -----------------------
if "memoria_estrella" not in st.session_state:
    st.session_state.memoria_estrella = []


def estrella_visual(color_lider, n_puntos=600, bubble_shell=True, all_lit=False):
    colores = {
        "dorado": "#F5C26B",
        "azul": "#4A90E2",
        "rojo": "#E26D5A"
    }

    rng = random.Random(42)
    t = time.time()
    pulse = 0.5 + 0.5 * math.sin(t * 2.0)
    angle = t * 0.2

    def star_vertices(outer_r=1.0, inner_r=0.45, points=5):
        verts = []
        step = math.pi / points
        for i in range(points * 2):
            r = outer_r if i % 2 == 0 else inner_r
            ang = i * step - math.pi / 2
            verts.append((r * math.cos(ang), r * math.sin(ang)))
        return verts

    def point_in_polygon(x, y, poly):
        inside = False
        j = len(poly) - 1
        for i in range(len(poly)):
            xi, yi = poly[i]
            xj, yj = poly[j]
            intersect = ((yi > y) != (yj > y)) and (
                    x < (xj - xi) * (y - yi) / (yj - yi + 1e-9) + xi
            )
            if intersect:
                inside = not inside
            j = i
        return inside

    star = star_vertices()
    extra_per_color = 100
    base_per_color = n_puntos // 3
    remainder = n_puntos - (base_per_color * 3)
    total_points = n_puntos + (extra_per_color * 3)

    xs, ys, zs = [], [], []
    while len(xs) < total_points:
        x = rng.uniform(-1.1, 1.1)
        y = rng.uniform(-1.1, 1.1)
        if point_in_polygon(x, y, star):
            xs.append(x)
            ys.append(y)
            zs.append(rng.uniform(-0.25, 0.25))

    cos_a = math.cos(angle)
    sin_a = math.sin(angle)
    xs_rot = []
    ys_rot = []
    for x, y in zip(xs, ys):
        xs_rot.append(x * cos_a - y * sin_a)
        ys_rot.append(x * sin_a + y * cos_a)

    palette = ["rojo", "azul", "dorado"]
    colors = []
    sizes = []
    is_lider = []

    per_color_counts = [base_per_color + extra_per_color] * 3
    for i in range(remainder):
        per_color_counts[i] += 1

    for color, count in zip(palette, per_color_counts):
        for _ in range(count):
            colors.append(colores[color])
            lider = all_lit or (color == color_lider)
            is_lider.append(lider)
            if lider:
                sizes.append(rng.uniform(4.2, 6.2))
            else:
                sizes.append(rng.uniform(2.2, 3.8))

    fig = go.Figure()
    total = len(colors)
    fig.add_trace(go.Scatter3d(
        x=[xs_rot[i] for i in range(total) if not is_lider[i]],
        y=[ys_rot[i] for i in range(total) if not is_lider[i]],
        z=[zs[i] for i in range(total) if not is_lider[i]],
        mode="markers",
        marker=dict(
            size=[sizes[i] for i in range(total) if not is_lider[i]],
            color=[colors[i] for i in range(total) if not is_lider[i]],
            opacity=0.45,
            line=dict(width=0)
        ),
        hoverinfo="skip"
    ))
    fig.add_trace(go.Scatter3d(
        x=[xs_rot[i] for i in range(total) if is_lider[i]],
        y=[ys_rot[i] for i in range(total) if is_lider[i]],
        z=[zs[i] for i in range(total) if is_lider[i]],
        mode="markers",
        marker=dict(
            size=[sizes[i] * (1.0 + 0.12 * pulse) for i in range(total) if is_lider[i]],
            color=[colors[i] for i in range(total) if is_lider[i]],
            opacity=0.95,
            line=dict(width=0)
        ),
        hoverinfo="skip"
    ))
    fig.add_trace(go.Scatter3d(
        x=[xs_rot[i] for i in range(total) if is_lider[i]],
        y=[ys_rot[i] for i in range(total) if is_lider[i]],
        z=[zs[i] for i in range(total) if is_lider[i]],
        mode="markers",
        marker=dict(
            size=[sizes[i] * (1.35 + 0.2 * pulse) for i in range(total) if is_lider[i]],
            color=[colors[i] for i in range(total) if is_lider[i]],
            opacity=0.02,
            line=dict(width=0)
        ),
        hoverinfo="skip"
    ))

    glow_x = []
    glow_y = []
    glow_z = []
    for _ in range(60):
        r = rng.uniform(0.0, 0.25)
        ang = rng.uniform(0.0, math.tau)
        glow_x.append(r * math.cos(ang))
        glow_y.append(r * math.sin(ang))
        glow_z.append(rng.uniform(-0.08, 0.08))
    glow_xr = [x * cos_a - y * sin_a for x, y in zip(glow_x, glow_y)]
    glow_yr = [x * sin_a + y * cos_a for x, y in zip(glow_x, glow_y)]

    if all_lit:
        for c in palette:
            fig.add_trace(go.Scatter3d(
                x=glow_xr,
                y=glow_yr,
                z=glow_z,
                mode="markers",
                marker=dict(
                    size=8 + 4 * pulse,
                    color=colores[c],
                    opacity=0.015,
                    line=dict(width=0)
                ),
                hoverinfo="skip"
            ))
    else:
        fig.add_trace(go.Scatter3d(
            x=glow_xr,
            y=glow_yr,
            z=glow_z,
            mode="markers",
            marker=dict(
                size=8 + 4 * pulse,
                color=colores[color_lider],
                opacity=0.015,
                line=dict(width=0)
            ),
            hoverinfo="skip"
        ))

    # Burbuja envolvente: superficie translúcida (una sola burbuja)
    if bubble_shell:
        r = 1.85
        steps = 22
        theta_vals = [i * (math.tau / (steps - 1)) for i in range(steps)]
        phi_vals = [i * (math.pi / (steps - 1)) for i in range(steps)]
        shell_x = []
        shell_y = []
        shell_z = []
        for phi in phi_vals:
            row_x = []
            row_y = []
            row_z = []
            for theta in theta_vals:
                x = r * math.cos(theta) * math.sin(phi)
                y = r * math.sin(theta) * math.sin(phi)
                z = r * math.cos(phi)
                # rotación suave para alinear con la estrella
                xr = x * cos_a - y * sin_a
                yr = x * sin_a + y * cos_a
                row_x.append(xr)
                row_y.append(yr)
                row_z.append(z)
            shell_x.append(row_x)
            shell_y.append(row_y)
            shell_z.append(row_z)
        fig.add_trace(go.Surface(
            x=shell_x,
            y=shell_y,
            z=shell_z,
            opacity=0.02,
            showscale=False,
            colorscale=[[0, "#1C2433"], [1, "#1C2433"]],
            lighting=dict(ambient=1.0, diffuse=0.0, specular=0.0, roughness=1.0, fresnel=0.0),
            hoverinfo="skip"
        ))


    fig.update_layout(
        width=350,
        height=350,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        scene=dict(
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            zaxis=dict(visible=False),
            camera_eye=dict(x=1.4, y=1.2, z=0.8)
        ),
        showlegend=False
    )

    return fig


esfera_visual = estrella_visual

# ====== CUENTA + PREMIUM (ANTES DEL ANÁLISIS) ======
if "usuario" not in st.session_state:
    st.session_state["usuario"] = _usuario_invitado()

# Sincroniza el estado premium/autenticación desde base local.
if st.session_state["usuario"].get("autenticado", False):
    st.session_state["usuario"] = recargar_usuario(st.session_state["usuario"]["id"])

with st.sidebar.expander("Cuenta", expanded=True):
    usuario_ui = st.session_state["usuario"]
    if not usuario_ui.get("autenticado", False):
        st.caption("Inicia sesion o crea cuenta con correo Gmail para guardar estado y activar Premium.")
        modo_auth = st.radio(
            "Acceso",
            ["Iniciar sesión", "Crear cuenta"],
            key="auth_mode_v1",
            horizontal=True
        )

        if modo_auth == "Iniciar sesión":
            email_login = st.text_input("Correo Gmail", placeholder="tuusuario@gmail.com", key="login_email_v1")
            pass_login = st.text_input("Contraseña", type="password", key="login_pass_v1")
            if st.button("Entrar", key="btn_login_v1"):
                ok, msg, user_public = autenticar_usuario(email_login, pass_login)
                if ok and user_public:
                    st.session_state["usuario"] = user_public
                    st.success(msg)
                    rerun_app()
                else:
                    st.error(msg)
        else:
            username_reg = st.text_input("Usuario", key="reg_user_v1")
            email_reg = st.text_input("Correo Gmail", placeholder="tuusuario@gmail.com", key="reg_email_v1")
            pass_reg = st.text_input("Contraseña", type="password", key="reg_pass_v1")
            pass_reg2 = st.text_input("Confirmar contraseña", type="password", key="reg_pass2_v1")
            if st.button("Crear cuenta", key="btn_register_v1"):
                if pass_reg != pass_reg2:
                    st.error("Las contraseñas no coinciden.")
                else:
                    ok, msg, user_public = registrar_usuario(username_reg, email_reg, pass_reg)
                    if ok and user_public:
                        st.session_state["usuario"] = user_public
                        st.success(msg)
                        rerun_app()
                    else:
                        st.error(msg)
    else:
        premium_label = "Activo" if usuario_ui.get("es_premium", False) else "No activo"
        st.markdown(f"**Usuario:** {usuario_ui.get('username', '')}")
        st.caption(f"Correo: {usuario_ui.get('email', '')}")
        st.caption(f"Premium: {premium_label}")
        until_raw = usuario_ui.get("premium_until")
        if until_raw and usuario_ui.get("es_premium", False):
            until_dt = _parse_iso_utc(until_raw)
            if until_dt:
                st.caption(f"Vence: {until_dt.strftime('%Y-%m-%d %H:%M UTC')}")

        st.divider()
        st.markdown("**Premium**")
        st.caption("Compra simulada local: activa o renueva Premium en esta app.")
        plan = st.selectbox(
            "Plan",
            ["Mensual (30 días)", "Anual (365 días)"],
            key="premium_plan_v1"
        )
        plan_days = 30 if "30" in plan else 365
        if usuario_ui.get("es_premium", False):
            st.success("Tienes Premium activo.")
            cta_premium = "Renovar Premium"
        else:
            cta_premium = "Comprar Premium"
        if st.button(cta_premium, key="btn_buy_premium_v1"):
            ok, msg, user_public = comprar_premium_usuario(usuario_ui["id"], plan_days)
            if ok and user_public:
                st.session_state["usuario"] = user_public
                st.success(msg)
                rerun_app()
            else:
                st.error(msg)

        st.divider()
        st.markdown("**Control temporal Premium**")
        st.caption("Solo para pruebas internas: activar/desactivar sin pago.")
        c_on, c_off = st.columns(2)
        if c_on.button("Activar", key="btn_force_premium_on_v1"):
            ok, msg, user_public = set_premium_usuario(usuario_ui["id"], True, 30)
            if ok and user_public:
                st.session_state["usuario"] = user_public
                st.success(msg)
                rerun_app()
            else:
                st.error(msg)
        if c_off.button("Desactivar", key="btn_force_premium_off_v1"):
            ok, msg, user_public = set_premium_usuario(usuario_ui["id"], False, 0)
            if ok and user_public:
                st.session_state["usuario"] = user_public
                st.warning(msg)
                rerun_app()
            else:
                st.error(msg)

        if st.button("Cerrar sesión", key="btn_logout_v1"):
            st.session_state["usuario"] = _usuario_invitado()
            rerun_app()

usuario = st.session_state["usuario"]
es_premium = bool(usuario.get("es_premium", False))

# Auto-activa diagnostico cuando el usuario pasa de no premium a premium.
prev_premium = st.session_state.get("premium_prev_state", False)
if es_premium and not prev_premium:
    st.session_state["debug_v13"] = True
elif not es_premium:
    st.session_state["debug_v13"] = False
st.session_state["premium_prev_state"] = es_premium

if es_premium:
    with st.sidebar.expander("Premium Tools", expanded=False):
        st.session_state["debug_v13"] = st.toggle(
            "Mostrar diagnostico v1.3 (Azul/Dorado/Rojo)",
            value=st.session_state.get("debug_v13", False),
            help="Activa el panel de diagnostico avanzado."
        )

if "show_quick_guide" not in st.session_state:
    st.session_state["show_quick_guide"] = True

with st.sidebar.expander("Ayuda", expanded=False):
    if st.button("📘 Ver guía rápida", key="btn_open_quick_guide"):
        st.session_state["show_quick_guide"] = True

# -----------------------
# SIDEBAR ? CONFIGURACI?N                    #CARGAR DATOS
# -----------------------

st.sidebar.header("⚙️ Configuración de mercado")

zona = st.sidebar.selectbox(
    "🌍 Zona horaria",
    ["UTC", "Bogotá", "New York", "Londres", "Madrid"],
    key="zona"
)
st.sidebar.divider()

mercados = {
    "Forex": "FOREX",
    "Oro (XAU/USD)": "GC=F",
    "NASDAQ 100": "^NDX",
    "S&P 500": "^GSPC",
    "Crypto": "CRYPTO"
}

mercado_nombre = st.sidebar.selectbox(
    "Selecciona mercado",
    list(mercados.keys())
)
mercado_abierto = mercado_abierto_ahora(mercado_nombre)
if not mercado_abierto:
    st.sidebar.caption("Estado: Mercado cerrado (fin de semana, horario NY).")
else:
    st.sidebar.caption("Estado: Mercado abierto.")

crypto_symbol = None
forex_pair = None
binance_symbol = None
td_symbol = None
if mercado_nombre == "Crypto":
    with st.sidebar.expander("Seleccionar cripto", expanded=True):
        if "crypto_symbol_v2" not in st.session_state:
            st.session_state["crypto_symbol_v2"] = st.session_state.get("crypto_symbol", "BTC")
        if "crypto_symbol" in st.session_state:
            del st.session_state["crypto_symbol"]
        crypto_symbol = st.selectbox(
            "Criptomoneda",
            list(CRYPTO_MAP.keys()),
            index=0,
            key="crypto_symbol_v2"
        )
    crypto_cfg = CRYPTO_MAP.get(crypto_symbol, CRYPTO_MAP["BTC"])
    ticker = crypto_cfg["ticker"]
    binance_symbol = crypto_cfg["binance"]
    td_symbol = crypto_cfg["td"]
elif mercado_nombre == "Forex":
    with st.sidebar.expander("Seleccionar par Forex", expanded=True):
        if "forex_pair_v1" not in st.session_state:
            st.session_state["forex_pair_v1"] = "EUR/USD"
        forex_pair = st.selectbox(
            "Par",
            list(FOREX_MAP.keys()),
            index=list(FOREX_MAP.keys()).index(st.session_state["forex_pair_v1"])
            if st.session_state.get("forex_pair_v1") in FOREX_MAP else 0,
            key="forex_pair_v1"
        )
    forex_cfg = FOREX_MAP.get(forex_pair, FOREX_MAP["EUR/USD"])
    ticker = forex_cfg["ticker"]
    td_symbol = forex_cfg["td"]
else:
    ticker = mercados[mercado_nombre]
    td_symbol = TD_SYMBOLS.get(mercado_nombre)

timeframes = {
    "5 minutos": "5m",
    "15 minutos": "15m",
    "1 hora": "1h",
    "4 horas": "4h",
    "1 día": "1d"
}

timeframe_nombre = st.sidebar.selectbox(
    "Timeframe",
    list(timeframes.keys()),
    index=1,
    key="timeframe_nombre"
)
modo_lectura = st.sidebar.selectbox(
    "Motor de lectura",
    ["Actual (1 timeframe)", "Estructural (1D + 4H)"],
    index=0,
    key="modo_lectura_v13"
)
if modo_lectura == "Estructural (1D + 4H)":
    st.sidebar.caption("Dirección base en 1D y ejecución en 4H.")
auto_refresh = st.sidebar.toggle("Auto refresh (gráficos)", value=st.session_state.get("auto_refresh_charts", True), key="auto_refresh_charts")
REFRESH_MS = 10000
STAR_REFRESH_SEC = 30
live_mode_enabled = st.sidebar.toggle(
    "Live Mode con Binance",
    value=st.session_state.get("live_mode_enabled", False),
    disabled=(mercado_nombre != "Crypto")
)
st.session_state["live_mode_enabled"] = bool(live_mode_enabled)
if mercado_nombre != "Crypto":
    st.sidebar.caption("Live Mode solo disponible para Crypto.")
intervalo = timeframes[timeframe_nombre]

if st.sidebar.button("🧹 Limpiar memoria de la Estrella"):
    st.session_state.memoria_estrella = []
    st.sidebar.success("Memoria limpiada.")

if mercado_abierto:
    st.caption(explicacion_horario(zona))
else:
    st.caption("Mercado cerrado (fin de semana). Cuando Habra, evalúa la sesión y la calidad del horario.")

if st.session_state.get("show_quick_guide", False):
    st.markdown("<div class='et-guide-backdrop'></div>", unsafe_allow_html=True)
    with st.container(key="onboarding_modal"):
        col_title, col_close = st.columns([4, 2])
        with col_title:
            st.markdown("### 📘 Guia rapida ")
        with col_close:
            if st.button("Entendido, cerrar guia", key="btn_close_quick_guide"):
                st.session_state["show_quick_guide"] = False
                rerun_app()
        st.markdown(cargar_guia_rapida_markdown())

# -----------------------
# TÍTULO
# -----------------------

def _img_to_base64(path: str) -> str:
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("ascii")
    except Exception:
        return ""

_title_star_path = os.path.join(os.path.dirname(__file__), "assets", "estrella Red.png")
_bg_star_path = os.path.join(os.path.dirname(__file__), "assets", "estrella trade.png")
_title_star_b64 = _img_to_base64(_title_star_path)
_bg_star_b64 = _img_to_base64(_bg_star_path)
if _bg_star_b64:
    st.markdown(
        f"""
        <style>
        .stApp {{
          background-image:
            linear-gradient(rgba(15,17,23,0.92), rgba(15,17,23,0.92)),
            url("data:image/png;base64,{_bg_star_b64}");
          background-size: auto 180%;
          background-position: center top;
          background-repeat: no-repeat;
          background-attachment: scroll;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )
if _title_star_b64:
    st.markdown(
        f"""
        <div style="display:flex; align-items:center; gap:14px;">
          <img src="data:image/png;base64,{_title_star_b64}" class="et-rotating-star" style="width:72px; height:72px; border-radius:22px; object-fit:cover; box-shadow:0 6px 18px rgba(0,0,0,0.35);" />
          <h1 style="margin:0;">Estrella Trader</h1>
        </div>
        """,
        unsafe_allow_html=True
    )
else:
    st.title("Estrella Trader")
st.caption("Instructor técnico y compañero de trading")

# Reserva visual para que el contenido no quede debajo del panel flotante (solo desktop)
st.markdown("<div style='height: 6px;'></div>", unsafe_allow_html=True)
st.markdown("<div style='margin-right: 360px;'></div>", unsafe_allow_html=True)

# -----------------------
# SESIÓN
# -----------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("⏰ Sesión actual")
    sesion = sesion_actual(zona) if mercado_abierto else "Mercado cerrado"
    st.write(sesion)

with col2:
    st.subheader("📊 Calidad del horario")
    calidad = calidad_horario(zona) if mercado_abierto else "cerrado"
    calidad_txt = calidad
    col_calidad, col_guardar = st.columns([3, 1])
    with col_calidad:
        st.write(calidad_txt)
    with col_guardar:
        guardar_notas = st.button("Guardar notas")

# ========= DATOS DE MERCADO =========

mercado = mercado_nombre

# 2) Mapear timeframes UI -> yfinance (IMPORTANTE)
INTERVAL_MAP = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "1h": "60m",  # yfinance suele ir mejor con 60m que "1h"
}

# 3) Periodo recomendado según intervalo (para evitar vacíos)
PERIOD_MAP = {
    "1m": "1d",
    "5m": "5d",
    "15m": "5d",
    "1h": "3mo",
    "60m": "3mo",
    "4h": "12mo",
    "1d": "3y",
}
periodo = PERIOD_MAP.get(intervalo, "5d")
modo_estructural = modo_lectura == "Estructural (1D + 4H)"
datos_1d = None
datos_4h = None

try:
    TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
    use_binance_live = bool(st.session_state.get("live_mode_enabled")) and mercado == "Crypto" and not modo_estructural
    if modo_estructural and bool(st.session_state.get("live_mode_enabled")) and mercado == "Crypto":
        st.sidebar.caption("Live Mode se desactiva en lectura estructural 1D+4H.")
    if use_binance_live and not WEBSOCKETS_AVAILABLE:
        st.sidebar.warning("Live Mode requiere el paquete websockets. Instálalo y reinicia.")
        use_binance_live = False
    if not use_binance_live:
        stop_prev = st.session_state.get("binance_stop")
        if stop_prev:
            stop_prev.set()

    if use_binance_live:
        if "binance_store" not in st.session_state:
            st.session_state["binance_store"] = BinanceLiveStore()
        store = st.session_state["binance_store"]

        if not binance_symbol:
            binance_symbol = "BTCUSDT"
        binance_interval = intervalo

        if store.symbol != binance_symbol or store.interval != binance_interval or store.get_df().empty:
            seed = fetch_klines(binance_symbol, binance_interval, limit=500)
            store.seed(seed, binance_symbol, binance_interval)

        current_params = (binance_symbol, binance_interval)
        needs_thread = (
            "binance_thread" not in st.session_state
            or not st.session_state["binance_thread"].is_alive()
            or st.session_state.get("binance_params") != current_params
        )
        if needs_thread:
            stop_prev = st.session_state.get("binance_stop")
            if stop_prev:
                stop_prev.set()
            thread, stop_event = start_stream(binance_symbol, binance_interval, store)
            st.session_state["binance_thread"] = thread
            st.session_state["binance_stop"] = stop_event
            st.session_state["binance_params"] = current_params

        datos = store.get_df()
        if datos.empty:
            raise ValueError("No se pudieron obtener datos live de Binance")
    else:
        td_interval = TD_INTERVAL_MAP.get(intervalo)
        datos = obtener_datos_procesados_cache(
            ticker=ticker,
            periodo=periodo,
            intervalo=intervalo,
            td_symbol_value=td_symbol,
            td_interval_value=td_interval,
            td_api_key_value=TD_API_KEY,
            zona_ui=zona,
        )
    if use_binance_live:
        datos = calcular_indicadores(datos)
        datos["dist_ema200"] = ((datos["Close"] - datos["EMA_200"]) / datos["EMA_200"]) * 100
        datos = ajustar_timezone(datos, zona)

    if modo_estructural:
        datos_1d = obtener_datos_procesados_cache(
            ticker=ticker,
            periodo="3y",
            intervalo="1d",
            td_symbol_value=td_symbol,
            td_interval_value=TD_INTERVAL_MAP.get("1d"),
            td_api_key_value=TD_API_KEY,
            zona_ui=zona,
        )
        datos_4h = obtener_datos_procesados_cache(
            ticker=ticker,
            periodo="12mo",
            intervalo="4h",
            td_symbol_value=td_symbol,
            td_interval_value=TD_INTERVAL_MAP.get("4h"),
            td_api_key_value=TD_API_KEY,
            zona_ui=zona,
        )
except ValueError as e:
    st.error(f"No se pudieron obtener datos del mercado: {e}")
    st.stop()  # Detiene la ejecucion para evitar errores posteriores
except Exception as e:
    st.error(f"Error inesperado obteniendo datos de mercado: {e}")
    st.stop()

# -----------------------
# ANÁLISIS DE CONTEXTO
# -----------------------

tendencia, estado_rsi = contexto_mercado(datos)
estado_bb = interpretar_bollinger(datos)
sesion = sesion_actual(zona) if mercado_abierto else "Mercado cerrado"
col_bb, col_notas = st.columns([1, 1])
with col_bb:
    st.dataframe(
        datos[["BBL", "BBM", "BBU"]].tail(3),
        use_container_width=False,
        width=420,
        height=140
    )
with col_notas:
    notas_bollinger = st.text_area(
        "",
        key="notas_bollinger",
        height=70,
        label_visibility="collapsed",
        placeholder="Notas"
    )
    if guardar_notas:
        base_dir = os.path.join(os.path.expanduser("~"), "Documents", "Archivos")
        os.makedirs(base_dir, exist_ok=True)
        nombre_archivo = f"notas_bollinger_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        ruta_salida = os.path.join(base_dir, nombre_archivo)
        with open(ruta_salida, "w", encoding="utf-8") as f:
            f.write(notas_bollinger or "")
        st.success(f"Notas guardadas en: {ruta_salida}")
estado_bb = interpretar_bollinger(datos)



def guardar_recuerdo(contexto):
    st.session_state.memoria_estrella.append(contexto)

    # límite de memoria (protección)
    if len(st.session_state.memoria_estrella) > 20:
        st.session_state.memoria_estrella.pop(0)


# -----------------------
# ESTADO DE LA ESTRELLA (N?CLEO)
# -----------------------
if "star_last_ts" not in st.session_state:
    st.session_state["star_last_ts"] = 0.0
if "star_force_refresh" not in st.session_state:
    st.session_state["star_force_refresh"] = True
if _HAS_AUTOREFRESH:
    st_autorefresh(interval=STAR_REFRESH_SEC * 1000, key="star_autorefresh")

st.subheader("Lectura de la Estrella")
if modo_estructural:
    st.caption("Modo estructural activo: dirección 1D + validación operativa 4H.")

now_ts = time.time()
recalc_estrella = (
    st.session_state.get("star_force_refresh", False)
    or (now_ts - st.session_state.get("star_last_ts", 0.0) >= STAR_REFRESH_SEC)
)
payload = st.session_state.get("star_payload")
if payload is None:
    recalc_estrella = True
analysis_signature = (
    modo_lectura,
    mercado_nombre,
    ticker,
    intervalo,
    zona,
    bool(use_binance_live),
)
if st.session_state.get("star_signature") != analysis_signature:
    recalc_estrella = True

if recalc_estrella:
    if modo_estructural:
        estado = construir_estado_estructural_cache(datos_1d, datos_4h)
    else:
        estado = construir_estado_final_cache(datos)

    if "dorado_hits" not in st.session_state:
        st.session_state["dorado_hits"] = 0
        st.session_state["dorado_miss"] = 0

    dorado = estado.get("dorado_v13")
    if dorado:
        st.session_state["dorado_hits"] += 1
    else:
        st.session_state["dorado_miss"] += 1

    if st.session_state.get("debug_v13", False):
        st.caption(f"🧪 Dorado hits: {st.session_state['dorado_hits']} | miss: {st.session_state['dorado_miss']}")
        st.write("DEBUG DORADO:", dorado)

    estado["esfera_v13"] = estado["esfera"]
    estado["frase_pedagogica_v13"] = estado["mensaje"]

    influencia = influencia_de_memoria(estado)

    razones = estado.get("razones", [])
    def _safe_float(val):
        if val is None or pd.isna(val):
            return None
        try:
            return float(val)
        except Exception:
            return None

    ultimo = datos.iloc[-1]
    estado["rsi"] = _safe_float(ultimo.get("RSI"))
    ema_200 = _safe_float(ultimo.get("EMA_200"))
    precio = _safe_float(ultimo.get("Close"))
    if ema_200 and precio:
        estado["cerca_ema200"] = abs(precio - ema_200) / ema_200 <= 0.01
    else:
        estado["cerca_ema200"] = False
    estado_bb = interpretar_bollinger(datos)
    estado["cerca_bollinger"] = estado_bb in ("ruptura_alcista", "ruptura_bajista")
    decision_ensenar = estado.get("decision", "OBSERVAR")
    if decision_ensenar == "OPERAR CON DISCIPLINA":
        decision_ensenar = "OPERAR"
    clasificados = clasificar_errores(influencia.get("errores", []))
    memoria_ensenar = {
        "indica_impulsividad": bool(clasificados.get("impulsividad"))
    }
    estado = aplicar_ensenar(
        estado=estado,
        decision=decision_ensenar,
        usuario=usuario,
        memoria=memoria_ensenar
    )
    resumen = estado.get("mensaje", "Resumen no disponible.")
    decision_txt = estado.get("decision", "OBSERVAR").upper()
    frase_pedagogica = estado.get("frase_pedagogica", estado.get("mensaje", ""))

    evento_estrella = {
        "fecha": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "sesion": sesion,
        "esfera": estado.get("esfera"),
        "riesgo": estado.get("riesgo"),
        "accion": estado.get("accion"),
        "decision": decision_txt,
        "resumen": resumen,
        "razones": razones,
        "nivel_memoria": influencia.get("nivel"),
    }

    st.session_state["star_payload"] = {
        "estado": estado,
        "influencia": influencia,
        "razones": razones,
        "resumen": resumen,
        "decision_txt": decision_txt,
        "frase_pedagogica": frase_pedagogica,
        "evento_estrella": evento_estrella,
    }
    st.session_state["star_last_ts"] = now_ts
    st.session_state["star_force_refresh"] = False
    st.session_state["star_signature"] = analysis_signature
else:
    estado = payload["estado"]
    influencia = payload["influencia"]
    razones = payload["razones"]
    resumen = payload["resumen"]
    decision_txt = payload["decision_txt"]
    frase_pedagogica = payload["frase_pedagogica"]
    evento_estrella = payload["evento_estrella"]

if not mercado_abierto:
    estado = estado.copy()
    estado.update({
        "decision": "MERCADO CERRADO",
        "accion": "ESPERAR APERTURA",
        "mensaje": "Mercado cerrado. Espera la apertura para una nueva lectura operativa.",
        "frase_pedagogica": "Mercado cerrado. Espera la apertura para una nueva lectura operativa.",
        "mensaje_direccion": "Fuera de sesion: la lectura operativa se reanuda en apertura.",
        "dorado_v13": None,
        "rojo_v13": None,
    })
    resumen = estado["mensaje"]
    decision_txt = estado["decision"]
    frase_pedagogica = estado["frase_pedagogica"]

if es_premium and st.session_state.get("debug_v13", False):
    with st.expander("Diagnostico Premium (v1.3)", expanded=False):
        debug = resumen_estado_humano(estado, usuario)

        d = debug["direccion"]
        st.markdown(f"### 🌟 Dirección dominante: **{d['valor']}** ({d['fortaleza']})")
        st.caption(
            f"Score alcista: {d['score_alcista']} | "
            f"Score bajista: {d['score_bajista']} | "
            f"Umbral: {d['umbral']}"
        )
        estructura = estado.get("estructura_1d_4h")
        if isinstance(estructura, dict):
            st.caption(
                f"1D: {estructura.get('direccion_1d', 'NEUTRAL')} | "
                f"4H: {estructura.get('direccion_4h', 'NEUTRAL')} | "
                f"Alineación: {estructura.get('alineacion', 'N/A')}"
            )

        dor = debug["dorado"]
        if dor["activo"]:
            st.markdown("### 🟡 Dorado: **ACTIVO**")
            if dor["micro_score"] is not None and dor["umbral"] is not None:
                st.caption(f"Micro-score: {dor['micro_score']} | Umbral: {dor['umbral']}")
            if dor["razones"]:
                st.write("• " + "\n• ".join(dor["razones"]))
        else:
            st.markdown("### 🟡 Dorado: **NO ACTIVO**")
            st.caption("No hay ventaja suficiente (esto es correcto).")

        r = debug["rojo"]
        if r["nivel"]:
            st.markdown(f"### 🔴 Rojo (riesgo): **{r['nivel']}**")
            if r["razones"]:
                st.write("• " + "\n• ".join(r["razones"]))
        else:
            st.markdown("### 🔴 Rojo (riesgo): —")
            st.caption("Rojo aparece cuando hay evaluación de riesgo disponible (idealmente con Dorado activo).")

        e = debug["ensenar"]
        if e["premium"]:
            st.markdown("### 🧭 Enseñar")
            st.caption("Disponible para tu cuenta.")
            st.write("Activo ✅" if e["activo"] else "Sin enseñanza relevante en este momento.")
            if e["titulo"]:
                st.caption(f"Título: {e['titulo']}")

# ========= [PANEL PRINCIPAL FLOTANTE - INICIO] =========
decision = estado.get("decision", "OBSERVAR")
frase = estado.get("frase_pedagogica", "Estructura válida, intención no clara.")
col_dec = color_por_decision(decision)
pos_map = {
    "Arriba derecha": "pos-tr",
    "Arriba izquierda": "pos-tl",
    "Abajo derecha": "pos-br",
    "Abajo izquierda": "pos-bl"
}
pos_class = pos_map.get(st.session_state.get("panel_pos", "Arriba derecha"), "pos-tr")
btn_style_map = {
    "Arriba derecha": "top: 224px; right: 6px;",
    "Arriba izquierda": "top: 224px; left: 36px;",
    "Abajo derecha": "bottom: 154px; right: 6px;",
    "Abajo izquierda": "bottom: 154px; left: 36px;",
}
btn_style = btn_style_map.get(st.session_state.get("panel_pos", "Arriba derecha"), "top: 224px; right: 6px;")

panel_placeholder = st.empty()
last_panel = st.session_state.get("last_panel_html")
if last_panel:
    panel_placeholder.markdown(last_panel, unsafe_allow_html=True)

debug_line = ""
if st.session_state.get("debug_v13", False):
    if not mercado_abierto:
        debug_line = "<div class='small-muted' style=\"font-family: 'Georgia', 'Times New Roman', serif; font-style: italic;\">Mercado cerrado - lectura operativa en pausa.</div>"
    else:
        esfera_dbg = estado.get("esfera", "")
        dir_dbg = (estado.get("direccion_v13") or "").upper()
        if "🔴" in esfera_dbg:
            debug_line = "<div class='small-muted' style=\"font-family: 'Georgia', 'Times New Roman', serif; font-style: italic;\">Rojo - Riesgo elevado, no operar.</div>"
        elif "🟡" in esfera_dbg:
            if dir_dbg in ("ALCISTA", "BAJISTA"):
                debug_line = f"<div class='small-muted' style=\"font-family: 'Georgia', 'Times New Roman', serif; font-style: italic;\">Dorado - Ventaja {dir_dbg.lower()}.</div>"
            else:
                debug_line = "<div class='small-muted' style=\"font-family: 'Georgia', 'Times New Roman', serif; font-style: italic;\">Dorado - Ventaja detectada.</div>"
        else:
            debug_line = "<div class='small-muted' style=\"font-family: 'Georgia', 'Times New Roman', serif; font-style: italic;\">Azul - Recomienda observar.</div>"

decision_icon = "👀"
if (decision or "").upper() == "MERCADO CERRADO":
    decision_icon = "⏸️"

panel_html = f"""
<div class="et-float-panel {pos_class}">
  <div class="et-card" style="margin:0;">
    <div class="et-title">Panel principal</div>
    <div class="et-decision" style="color:{col_dec};">{decision_icon} {decision}</div>
    {debug_line}
  </div>
</div>
"""
if panel_html != last_panel:
    panel_placeholder.markdown(panel_html, unsafe_allow_html=True)
    st.session_state["last_panel_html"] = panel_html

st.markdown(f"""
<style>
.st-key-refresh_float_panel {{
  position: fixed;
  z-index: 10001;
  width: 212px;
  {btn_style}
}}
.st-key-refresh_float_panel .stButton>button {{
  width: 100%;
  border-radius: 12px;
  border: 1px solid #5BE36A;
  background: linear-gradient(135deg, #7CFF8A 0%, #39D353 100%);
  color: #0A2610;
  font-weight: 700;
  font-family: "Segoe UI", Tahoma, Arial, sans-serif;
  letter-spacing: 0.2px;
  box-shadow: 0 8px 22px rgba(57, 211, 83, 0.45);
}}
.st-key-refresh_float_panel .stButton>button:hover {{
  filter: brightness(1.04);
  border-color: #A4FFAF;
}}
@media (max-width: 900px){{
  .st-key-refresh_float_panel {{
    left: 28px !important;
    right: 28px !important;
    top: auto !important;
    bottom: 132px !important;
    width: auto !important;
  }}
}}
</style>
""", unsafe_allow_html=True)

with st.container(key="refresh_float_panel"):
    if st.button("Actualizar lectura", key="btn_actualizar_lectura_panel"):
        st.session_state["star_force_refresh"] = True
# ========= [PANEL PRINCIPAL FLOTANTE - FIN] =========

if not mercado_abierto:
    st.subheader("⏸️ Mercado cerrado")
    st.write("La lectura operativa queda en pausa hasta la apertura del mercado.")
elif estado.get("dorado_v13"):
    st.subheader("🟡 Ventaja detectada (Dorado)")
    st.write(estado["dorado_v13"]["resumen"])
    st.write("Acción:", estado["dorado_v13"]["accion"])
    st.write("RR estimado:", estado["dorado_v13"]["rr_estimado"])
    st.write("Razones:")
    for r in estado["dorado_v13"]["razones"]:
        st.write("•", r)
else:
    st.subheader("🔵 Azul")
    st.write(estado.get("mensaje", "No hay ventaja suficiente ahora."))

if estado.get("dorado_v13") and estado.get("rojo_v13"):
    st.subheader("🔴 Riesgo (Rojo)")
    st.write(f"Nivel: **{estado['rojo_v13']['nivel']}**")
    if estado["rojo_v13"]["razones"]:
        st.write("Razones:")
        for r in estado["rojo_v13"]["razones"]:
            st.write("•", r)

if recalc_estrella and mercado_abierto:
    guardar_recuerdo(evento_estrella)

# ========= [MENSAJE ESTRELLA - INICIO] =========
esfera = estado.get("esfera", "🔵 Azul (análisis)")
mensaje = estado.get("mensaje", "")
mensaje = "\n".join([line for line in mensaje.splitlines() if line.strip()])

accent = color_por_esfera(esfera)
msg_placeholder = st.empty()
last_msg = st.session_state.get("last_star_msg_html")
msg_html = f"""
<div class="et-star-msg" style="--accent:{accent};">
  <div class="et-title">📣 Mensaje de la Estrella</div>
  <div style="white-space: pre-line;">{mensaje}</div>
</div>
"""
if msg_html != last_msg:
    msg_placeholder.markdown(msg_html, unsafe_allow_html=True)
    st.session_state["last_star_msg_html"] = msg_html
elif last_msg:
    msg_placeholder.markdown(last_msg, unsafe_allow_html=True)
# ========= [MENSAJE ESTRELLA - FIN] =========


# ========= [AUTO-GUARDADO EVENTOS MERCADO - INICIO] =========
if "memoria_eventos" not in st.session_state:
    st.session_state["memoria_eventos"] = []

eventos_mercado = estado.get("eventos_mercado", []) or []

# Guardar solo si hay eventos y evitar duplicados por fecha+tipo+mercado
for ev in eventos_mercado:
    firma = (ev.get("fecha"), ev.get("tipo"), ev.get("mercado"))
    ya = any((m.get("fecha"), m.get("tipo"), m.get("mercado")) == firma for m in st.session_state["memoria_eventos"])
    if not ya:
        st.session_state["memoria_eventos"].append(ev)

# Limitar tamaño (evitar crecimiento infinito)
st.session_state["memoria_eventos"] = st.session_state["memoria_eventos"][-200:]
# ========= [AUTO-GUARDADO EVENTOS MERCADO - FIN] =========


# RAZONES + TRAZABILIDAD (EN FRENTE)
# -----------------------
col_razones, col_traza = st.columns(2)

with col_razones:
    # ========= [RAZONES - INICIO] =========
    razones = estado.get("razones", [])
    if False:
        st.markdown('<div class="et-card"><div class="et-title">🧠 Razones de la Estrella</div>', unsafe_allow_html=True)
        if razones:
            st.markdown('<div class="et-row">', unsafe_allow_html=True)
            for r in razones:
                st.markdown(f'<span class="et-pill">{r}</span>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="small-muted">Sin razones registradas.</span>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    # ========= [RAZONES - FIN] =========

with col_traza:
    # ========= [TRAZABILIDAD - INICIO] =========
    evento = evento_estrella

    if evento:
        if False:
            if False:
                fecha = evento.get("fecha", "")
            sesion = evento.get("sesión", evento.get("sesion", ""))
            esfera = evento.get("esfera", "")
            riesgo = evento.get("riesgo", "")
            accion = evento.get("acción", evento.get("accion", ""))
            decision = evento.get("decisión", evento.get("decision", ""))
            resumen = evento.get("resumen", evento.get("Resumen", ""))

            accent = color_por_esfera(esfera)

            st.markdown(f"""
            <div class="et-card">
              <div class="et-title">🧾 Trazabilidad de la Estrella</div>

              <div class="et-row">
                <span class="et-pill">{fecha}</span>
                <span class="et-pill">{sesion}</span>
                <span class="et-pill">{esfera}</span>
              </div>

              <div class="et-kv">
                <b>Riesgo:</b> {riesgo} &nbsp; • &nbsp;
                <b>Acción:</b> {accion} &nbsp; • &nbsp;
                <b>Decisión:</b> {decision}
              </div>

              <div class="et-divider"></div>

              <div class="et-snippet" style="border-left:3px solid {accent}; padding-left:10px;">
                <span class="et-mini">Resumen</span><br>
                {resumen}
              </div>
            </div>
            """, unsafe_allow_html=True)
    # ========= [TRAZABILIDAD - FIN] =========

# ========= [RECUERDOS RELEVANTES - INICIO] =========
if "filtro_memoria_esfera" not in st.session_state:
    st.session_state["filtro_memoria_esfera"] = None

esfera_actual = estado.get("esfera", "")

memoria_base = st.session_state.memoria_estrella
filtro_esfera = st.session_state.get("filtro_memoria_esfera")
if filtro_esfera:
    memoria_base = [r for r in memoria_base if filtro_esfera in (r.get("esfera") or "")]
    esfera_filtro = filtro_esfera
else:
    esfera_filtro = estado["esfera"]

relevantes = filtrar_por_esfera(memoria_base, esfera_filtro)
relevantes = list(reversed(relevantes))[:5]
recuerdos = relevantes

if recuerdos:
    if False:
        st.markdown('<div class="et-card"><div class="et-title">🧠 Últimos recuerdos relevantes</div>',
                    unsafe_allow_html=True)

        r = recuerdos[0]
        fecha = r.get("fecha", "")
        sesion = r.get("sesión", r.get("sesion", ""))
        esfera = r.get("esfera", "")
        riesgo = r.get("riesgo", "")
        resumen = r.get("resumen", r.get("Resumen", ""))
        motivo = r.get("motivo", "")

        accent = color_por_esfera(esfera)

        st.markdown(f"""
        <div style="padding:10px 0;">
          <div class="et-row">
            <span class="et-pill">{fecha}</span>
            <span class="et-pill">{sesion}</span>
            <span class="et-pill">{esfera}</span>
          </div>

          <div class="et-kv"><b>Riesgo:</b> {riesgo}</div>

          <div class="et-snippet" style="border-left:3px solid {accent}; padding-left:10px;">
            <span class="et-mini">Resumen</span><br>
            {resumen}
          </div>

          {"<div class='et-kv'><b>Motivo:</b> " + motivo + "</div>" if motivo else ""}
        </div>
        """, unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)
# ========= [RECUERDOS RELEVANTES - FIN] =========
st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

# -----------------------
# ENSEÑAR (DESPUÉS DE RECUERDOS)
# -----------------------
ens = estado.get("ensenar")
if es_premium and (ens or ("🔴" in esfera_actual or "🟡" in esfera_actual)):
    st.markdown("<div class='et-title'>🧠 Enseñar</div>", unsafe_allow_html=True)

    if "🔴" in esfera_actual or "🟡" in esfera_actual:
        razon_usuario = st.text_input("¿Por qué NO operar aquí?", key="razon_usuario")
        if st.button("📚 Enseñar Estrella") and razon_usuario.strip():
            guardar_recuerdo({
                "fecha": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "sesion": sesion,
                "esfera": esfera_actual,
                "razon_usuario": razon_usuario.strip(),
                "nivel": 3
            })
            st.success("Recuerdo guardado en memoria.")

    if ens:
        titulo = normalizar_texto_ui(ens.get("titulo", ""))
        texto = normalizar_texto_ui(ens.get("texto", ""))
        cierre = normalizar_texto_ui(ens.get("cierre", ""))
        if not texto:
            partes = [
                normalizar_texto_ui(ens.get("ahora", "")),
                normalizar_texto_ui(ens.get("porque", "")),
                normalizar_texto_ui(ens.get("proximo", "")),
            ]
            texto = "\n".join([p for p in partes if p])

        st.markdown(f"**{titulo}**")
        st.write(texto)
        if cierre:
            st.caption(cierre)

# ========= [EVENTOS MERCADO RECIENTES - OPCIONAL] =========
if st.session_state["usuario"].get("es_premium", False):
    eventos = (st.session_state.get("memoria_eventos") or [])[-2:]
    if eventos:
        st.markdown("<div class='small-muted'>Eventos de mercado recientes:</div>", unsafe_allow_html=True)
        for ev in reversed(eventos):
            st.markdown(f"- {ev.get('fecha', '')} — {ev.get('titulo', '')} ({ev.get('sesion', '')})")

# -----------------------
# ADVERTENCIAS POR MEMORIA
# -----------------------
advertencia = advertencia_por_memoria(estado)
if advertencia:
    st.warning(f"🧠 Memoria de la Estrella:\n\n{advertencia}")

# -----------------------
# VISUALIZACIÓN (UI PURA)
# -----------------------
esfera_txt = estado.get("esfera", "")
if "🔴" in esfera_txt:
    ui = render_estado_estrella({"esfera": "ROJO"})
elif "🔵" in esfera_txt:
    ui = render_estado_estrella({"esfera": "AZUL"})
else:
    ui = render_estado_estrella({"esfera": "DORADO"})

debug_placeholder = st.empty()
debug_placeholder.empty()
# -----------------------
# ESTRELLA (VISUAL)
# -----------------------
dir_v13 = (estado.get("direccion_v13") or "").upper()
esfera_estado = estado.get("esfera", "")
if "🔴" in esfera_estado:
    esfera_visual_txt = "🔴"
elif "🟡" in esfera_estado:
    esfera_visual_txt = "🟡"
else:
    esfera_visual_txt = "🔵"

color_estado = "dorado"
if esfera_visual_txt == "🔴":
    color_estado = "rojo"
elif esfera_visual_txt == "🔵":
    color_estado = "azul"

st.plotly_chart(
    esfera_visual(color_estado, all_lit=not mercado_abierto),
    use_container_width=False,
    key="esfera_visual"
)
if esfera_visual_txt == "🔴":
    st.error("🔴 ESTRELLA EN MODO RIESGO — Riesgo sobre la ventaja")
elif esfera_visual_txt == "🟡":
    if dir_v13 == "ALCISTA":
        st.warning("🟡 ESTRELLA EN MODO TENDENCIA — Alcista")
    elif dir_v13 == "BAJISTA":
        st.warning("🟡 ESTRELLA EN MODO TENDENCIA — Bajista")
    else:
        st.warning("🟡 ESTRELLA EN MODO TENDENCIA — Ventaja detectada")
else:
    st.info("🔵 ESTRELLA EN MODO NEUTRAL — Observa")

# -----------------------
# GRÁFICO
# -----------------------
def _render_main_chart(datos, use_binance_live, ticker, chart_placeholder, mercado_abierto):
    if datos.empty:
        st.warning("No hay datos para el gráfico.")
        return

    df = datos.copy().dropna(subset=["Open", "High", "Low", "Close"])
    if df.empty:
        st.warning("No hay velas válidas para el gráfico.")
        return
    for span in (20, 50, 200):
        col = f"EMA_{span}"
        if col not in df.columns and "Close" in df.columns:
            df[col] = df["Close"].ewm(span=span, adjust=False).mean()

    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.6, 0.2, 0.2],
        subplot_titles=("Velas", "Volumen", "RSI (14)")
    )

    fig.add_trace(go.Candlestick(
        x=df.index,
        open=df["Open"],
        high=df["High"],
        low=df["Low"],
        close=df["Close"],
        name="Velas"
    ), row=1, col=1)

    for ema, color, width in [("EMA_20", "#4A90E2", 2), ("EMA_50", "#9CC6FF", 2), ("EMA_200", "#F5C26B", 3)]:
        if ema in df.columns:
            fig.add_trace(go.Scatter(
                x=df.index,
                y=df[ema],
                mode="lines",
                name=ema,
                line=dict(width=width, color=color)
            ), row=1, col=1)

    if "Volume" in df.columns:
        colors = ["#2ECC71" if c > o else "#E74C3C" for o, c in zip(df["Open"], df["Close"])]
        fig.add_trace(go.Bar(
            x=df.index,
            y=df["Volume"],
            name="Volumen",
            marker_color=colors,
            opacity=0.7
        ), row=2, col=1)

    if "RSI" in df.columns:
        fig.add_trace(go.Scatter(
            x=df.index,
            y=df["RSI"],
            mode="lines",
            name="RSI",
            line=dict(width=2, color="#B277FF")
        ), row=3, col=1)
        fig.add_hline(y=70, line_dash="dash", line_color="#E26D5A", row=3, col=1)
        fig.add_hline(y=50, line_dash="dot", line_color="#9AA1B2", row=3, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="#4A90E2", row=3, col=1)

    if not mercado_abierto:
        precio_live = None
    elif use_binance_live:
        precio_live = float(df["Close"].iloc[-1]) if not df.empty else None
    else:
        precio_live = None
        td_api_key = os.getenv("TWELVE_DATA_API_KEY")
        if td_api_key and td_symbol:
            precio_live = obtener_precio_live_twelvedata(td_symbol, td_api_key)
        if precio_live is None:
            precio_live = obtener_precio_live(ticker)
    if precio_live is not None:
        live_ts = datetime.now(pytz.timezone(TD_TZ_MAP.get(zona, "UTC")))
        fig.add_trace(go.Scatter(
            x=[live_ts],
            y=[precio_live],
            mode="markers",
            name="Precio LIVE",
            marker=dict(size=10, color="#FFD166")
        ), row=1, col=1)

    # Forzar la última vela centrada en el viewport (con espacio a ambos lados).
    if len(df.index) >= 2:
        paso = df.index[-1] - df.index[-2]
        if paso == pd.Timedelta(0):
            paso = pd.Timedelta(minutes=5)
    else:
        paso = pd.Timedelta(minutes=5)
    barras_lado = max(60, min(140, len(df) - 1))
    x_inicio = df.index[-1] - (paso * barras_lado)
    x_fin = df.index[-1] + (paso * barras_lado)
    fig.update_xaxes(range=[x_inicio, x_fin])

    fig.update_layout(
        template="plotly_dark",
        height=720,
        xaxis_rangeslider_visible=False,
        showlegend=False,
        dragmode="pan",
        margin=dict(l=10, r=10, t=30, b=10)
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(128,128,128,0.2)")
    with chart_placeholder.container():
        st.plotly_chart(
            fig,
            use_container_width=True,
            key="chart_main_velas",
            config={"scrollZoom": True}
        )
        if not mercado_abierto:
            st.caption("Mercado cerrado: se muestran velas históricas, sin precio live.")


# -----------------------
# RSI
# -----------------------
def _render_rsi_chart(datos):
    return


def _render_fragment(fn, *args):
    if hasattr(st, "fragment") and auto_refresh:
        st.fragment(run_every=REFRESH_MS / 1000)(fn)(*args)
    else:
        if auto_refresh and not hasattr(st, "fragment"):
            st.sidebar.warning("Auto refresh requiere Streamlit con st.fragment.")
        fn(*args)


def _render_star_section():
    pass


_render_fragment(_render_star_section)
main_chart_placeholder = st.empty()
_render_fragment(_render_main_chart, datos, use_binance_live, ticker, main_chart_placeholder, mercado_abierto)
_render_fragment(_render_rsi_chart, datos)













