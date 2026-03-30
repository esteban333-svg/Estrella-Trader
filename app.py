# -----------------------
# IMPORTS DEL PROYECTO DEJAR ARRIBA
# -----------------------
from analysis import obtener_precio_live
import streamlit as st
import streamlit.components.v1 as components
from estrella_ui import render_estado_estrella
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pytz
import requests
import pandas as pd
from analysis import interpretar_bollinger
import time
import math
import html
import os
import random
import base64
import json
import hashlib
import hmac
import smtplib
import logging
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
try:
    from streamlit_cookies_manager import EncryptedCookieManager
    _HAS_COOKIE_MANAGER = True
except Exception:
    EncryptedCookieManager = None
    _HAS_COOKIE_MANAGER = False
import streamlit as st

logger = logging.getLogger(__name__)


# -----------------------
# CONFIGURACION DE LA APP (SIEMPRE PRIMERO)
# -----------------------
st.set_page_config(
    page_title="Estrella Trader V1" ,
    layout="wide"
)


# ============================================================
# BLOQUE: TEMA GLOBAL Y ESTILOS (CSS)
# ============================================================

COL_BG = "#0F1117"
COL_CARD = "#161A23"
COL_TEXT = "#E6E8EE"
COL_TEXT_2 = "#9AA1B2"
COL_MUTE = "#6B7280"

COL_BLUE = "#4A90E2"
COL_GOLD = "#F5C26B"
COL_RED = "#E26D5A"
BG_STAR_SIZE_PX = 1900

st.markdown(f"""
<style>
/* Fondo general */
.stApp {{
  background-color: {COL_BG};
  background:
    radial-gradient(1200px 700px at 50% 18%, rgba(45,56,82,0.30), rgba(15,17,23,0.0) 62%),
    linear-gradient(rgba(15,17,23,0.96), rgba(15,17,23,0.96));
  color: {COL_TEXT};
}}

/* Velo oscuro para legibilidad del texto sobre la estrella */
.stApp::before {{
  content: "";
  position: fixed;
  inset: 0;
  z-index: 1;
  pointer-events: none;
  background: linear-gradient(rgba(8, 10, 16, 0.46), rgba(8, 10, 16, 0.62));
}}

/* Capa Plotly de fondo (estrella) */
.st-key-bg_star_full {{
  position: fixed !important;
  inset: 0 !important;
  z-index: 0 !important;
  pointer-events: none !important;
  opacity: 0.42 !important;
  width: 100vw !important;
  height: 100vh !important;
  overflow: hidden !important;
  display: flex !important;
  align-items: center !important;
  justify-content: center !important;
  filter: saturate(0.92) brightness(0.92) !important;
}}

.st-key-bg_star_full > div {{
  height: 100vh !important;
  width: 100vw !important;
}}

.st-key-bg_star_full [data-testid="stPlotlyChart"] {{
  position: static !important;
  left: auto !important;
  top: auto !important;
  transform: none !important;
  width: {BG_STAR_SIZE_PX}px !important;
  max-width: none !important;
  height: {BG_STAR_SIZE_PX}px !important;
}}

.st-key-bg_star_full .js-plotly-plot,
.st-key-bg_star_full .plot-container,
.st-key-bg_star_full .svg-container {{
  width: {BG_STAR_SIZE_PX}px !important;
  max-width: none !important;
  height: {BG_STAR_SIZE_PX}px !important;
}}

.block-container {{
  position: relative;
  z-index: 2;
}}

[data-testid="stSidebar"] {{
  position: relative;
  z-index: 3;
}}

[data-testid="stExpander"] {{
  background: rgba(12, 16, 28, 0.52);
  border: 1px solid rgba(255,255,255,0.10);
  border-radius: 12px;
}}

/* Núcleo de luz central (efecto observando) */
.et-core-watch {{
  position: fixed;
  left: 50%;
  top: 50%;
  width: min(10vw, 140px);
  aspect-ratio: 1 / 1;
  transform: translate(-50%, -50%);
  z-index: 1;
  pointer-events: none;
  border-radius: 50%;
  background:
    radial-gradient(circle, rgba(255,255,255,0.78) 0%, rgba(255,255,255,0.34) 26%, rgba(74,144,226,0.12) 58%, rgba(0,0,0,0) 100%);
  box-shadow: 0 0 42px rgba(255,255,255,0.24), 0 0 110px rgba(74,144,226,0.10);
  filter: blur(1.2px);
  opacity: 0.22;
  animation: et-core-watch-pulse 7.4s ease-in-out infinite;
}}

@keyframes et-core-watch-pulse {{
  0%, 100% {{ opacity: 0.16; transform: translate(-50%, -50%) scale(0.98); }}
  50% {{ opacity: 0.28; transform: translate(-50%, -50%) scale(1.03); }}
}}

/* Ambiente galaxia persistente (muy ligero) */
.et-galaxy-ambient {{
  position: fixed;
  inset: 0;
  z-index: 1;
  pointer-events: none;
  overflow: hidden;
}}

.et-galaxy-blob {{
  position: absolute;
  width: min(42vw, 560px);
  aspect-ratio: 1 / 1;
  border-radius: 999px;
  mix-blend-mode: screen;
  filter: blur(26px);
  will-change: transform, opacity;
}}

.et-galaxy-b1 {{
  left: -8vw;
  top: 18vh;
  background: radial-gradient(circle, rgba(74,144,226,0.16), rgba(74,144,226,0.02) 62%, rgba(74,144,226,0) 100%);
  animation: et-galaxy-drift-a 18s ease-in-out infinite;
}}

.et-galaxy-b2 {{
  right: -10vw;
  top: 28vh;
  background: radial-gradient(circle, rgba(226,109,90,0.14), rgba(226,109,90,0.02) 62%, rgba(226,109,90,0) 100%);
  animation: et-galaxy-drift-b 21s ease-in-out infinite;
}}

.et-galaxy-b3 {{
  left: 24vw;
  bottom: -14vh;
  background: radial-gradient(circle, rgba(245,194,107,0.14), rgba(245,194,107,0.02) 62%, rgba(245,194,107,0) 100%);
  animation: et-galaxy-drift-c 20s ease-in-out infinite;
}}

/* Intro one-shot al login: esferas se separan */
.et-login-intro {{
  position: fixed;
  inset: 0;
  z-index: 12;
  pointer-events: none;
  overflow: hidden;
  animation: et-login-intro-out 3.2s ease forwards;
}}

.et-login-intro-core {{
  position: absolute;
  left: 50%;
  top: 50%;
  width: 120px;
  height: 120px;
  margin-left: -60px;
  margin-top: -60px;
  border-radius: 50%;
  background: radial-gradient(circle, rgba(255,255,255,0.95), rgba(255,255,255,0.56) 42%, rgba(255,255,255,0.08) 78%, rgba(255,255,255,0) 100%);
  box-shadow: 0 0 28px rgba(255,255,255,0.28), 0 0 84px rgba(255,255,255,0.18);
  will-change: transform, opacity;
  animation: et-login-core-pulse 3.2s ease forwards;
}}

.et-login-orb {{
  position: absolute;
  left: 50%;
  top: 50%;
  width: 180px;
  height: 180px;
  margin-left: -90px;
  margin-top: -90px;
  border-radius: 50%;
  mix-blend-mode: screen;
  will-change: transform, opacity;
}}

.et-login-orb-r {{
  background: radial-gradient(circle, rgba(255,255,255,0.50), rgba(226,109,90,0.44) 40%, rgba(226,109,90,0.08) 72%, rgba(226,109,90,0) 100%);
  animation: et-login-orb-r 3.2s cubic-bezier(0.2, 0.85, 0.2, 1) forwards;
}}

.et-login-orb-b {{
  background: radial-gradient(circle, rgba(255,255,255,0.50), rgba(74,144,226,0.44) 40%, rgba(74,144,226,0.08) 72%, rgba(74,144,226,0) 100%);
  animation: et-login-orb-b 3.2s cubic-bezier(0.2, 0.85, 0.2, 1) forwards;
}}

.et-login-orb-g {{
  background: radial-gradient(circle, rgba(255,255,255,0.52), rgba(245,194,107,0.44) 40%, rgba(245,194,107,0.08) 72%, rgba(245,194,107,0) 100%);
  animation: et-login-orb-g 3.2s cubic-bezier(0.2, 0.85, 0.2, 1) forwards;
}}

/* Lluvia de estrellas one-shot */
.et-star-rain-overlay {{
  position: fixed;
  inset: 0;
  z-index: 10022;
  pointer-events: none;
  overflow: hidden;
  animation: et-star-rain-fade 6.4s ease forwards;
}}

.et-star-rain-core {{
  position: fixed;
  left: var(--x0);
  top: var(--y0);
  width: 74px;
  height: 74px;
  margin-left: -37px;
  margin-top: -37px;
  border-radius: 50%;
  background: radial-gradient(circle, rgba(255,255,255,0.96), rgba(255,255,255,0.36) 52%, rgba(255,255,255,0.0) 100%);
  box-shadow: 0 0 24px rgba(255,255,255,0.22), 0 0 62px rgba(74,144,226,0.20);
  opacity: 0.0;
  animation: et-star-rain-core 5.8s ease forwards;
}}

.et-star-rain-dot {{
  position: fixed;
  left: var(--x0);
  top: var(--y0);
  width: var(--s);
  height: var(--s);
  border-radius: 999px;
  background: var(--c);
  box-shadow: 0 0 8px rgba(255,255,255,0.28);
  opacity: 0;
  will-change: transform, opacity;
  animation: et-star-rain-merge var(--dur) cubic-bezier(0.18, 0.82, 0.2, 1) forwards;
  animation-delay: var(--d);
}}

@keyframes et-star-rain-merge {{
  0% {{ transform: translate(var(--sx), var(--sy)) scale(0.90); opacity: 0; }}
  12% {{ opacity: 0.96; }}
  58% {{ transform: translate(var(--ex), var(--ey)) scale(1.04); opacity: 0.98; }}
  76% {{ transform: translate(var(--ex), var(--ey)) scale(1.00); opacity: 0.92; }}
  100% {{ transform: translate(var(--bx), var(--by)) scale(0.28); opacity: 0.04; }}
}}

@keyframes et-star-rain-core {{
  0% {{ opacity: 0.0; transform: scale(0.36); }}
  46% {{ opacity: 0.84; transform: scale(1.10); }}
  72% {{ opacity: 0.66; transform: scale(1.00); }}
  100% {{ opacity: 0.08; transform: scale(0.24); }}
}}

@keyframes et-star-rain-fade {{
  0%, 92% {{ opacity: 1; }}
  100% {{ opacity: 0; }}
}}

/* Intro login: cae + se reagrupa formando estrella */
.et-login-starstorm {{
  position: fixed;
  inset: 0;
  z-index: 10020;
  pointer-events: none;
  overflow: hidden;
  animation: et-login-starstorm-fade 7.2s ease forwards;
}}

.et-login-star-dot {{
  position: fixed;
  left: 50%;
  top: 50%;
  width: var(--s);
  height: var(--s);
  border-radius: 999px;
  background: var(--c);
  box-shadow: 0 0 8px rgba(255,255,255,0.26);
  opacity: 0;
  will-change: transform, opacity;
  animation: et-login-star-merge var(--dur) cubic-bezier(0.16, 0.84, 0.20, 1) forwards;
  animation-delay: var(--d);
}}

.et-login-star-core {{
  position: fixed;
  left: 50%;
  top: 50%;
  width: 112px;
  height: 112px;
  transform: translate(-50%, -50%);
  border-radius: 50%;
  background: radial-gradient(circle, rgba(255,255,255,0.92), rgba(255,255,255,0.34) 52%, rgba(255,255,255,0.0) 100%);
  box-shadow: 0 0 34px rgba(255,255,255,0.24), 0 0 84px rgba(74,144,226,0.18);
  opacity: 0.92;
  animation: et-login-star-core 6.8s ease forwards;
}}

@keyframes et-login-star-merge {{
  0% {{ transform: translate(var(--sx), var(--sy)) scale(0.90); opacity: 0; }}
  12% {{ opacity: 0.94; }}
  50% {{ transform: translate(var(--ex), var(--ey)) scale(1.04); opacity: 0.98; }}
  72% {{ transform: translate(var(--ex), var(--ey)) scale(1.0); opacity: 0.95; }}
  100% {{ transform: translate(var(--bx), var(--by)) scale(0.20); opacity: 0.06; }}
}}

@keyframes et-login-star-core {{
  0% {{ opacity: 0.0; transform: translate(-50%, -50%) scale(0.38); }}
  42% {{ opacity: 0.88; transform: translate(-50%, -50%) scale(1.16); }}
  68% {{ opacity: 0.72; transform: translate(-50%, -50%) scale(1.00); }}
  100% {{ opacity: 0.10; transform: translate(-50%, -50%) scale(0.18); }}
}}

@keyframes et-login-starstorm-fade {{
  0%, 90% {{ opacity: 1; }}
  100% {{ opacity: 0; }}
}}

@keyframes et-galaxy-drift-a {{
  0%, 100% {{ transform: translate3d(0, 0, 0) scale(1); opacity: 0.24; }}
  50% {{ transform: translate3d(34px, -18px, 0) scale(1.08); opacity: 0.34; }}
}}

@keyframes et-galaxy-drift-b {{
  0%, 100% {{ transform: translate3d(0, 0, 0) scale(1); opacity: 0.18; }}
  50% {{ transform: translate3d(-36px, 24px, 0) scale(1.10); opacity: 0.30; }}
}}

@keyframes et-galaxy-drift-c {{
  0%, 100% {{ transform: translate3d(0, 0, 0) scale(1); opacity: 0.16; }}
  50% {{ transform: translate3d(18px, -26px, 0) scale(1.06); opacity: 0.26; }}
}}

@keyframes et-login-intro-out {{
  0%, 74% {{ opacity: 1; }}
  100% {{ opacity: 0; }}
}}

@keyframes et-login-core-pulse {{
  0% {{ transform: scale(0.38); opacity: 0.96; }}
  35% {{ transform: scale(1.22); opacity: 0.78; }}
  74% {{ transform: scale(0.72); opacity: 0.44; }}
  100% {{ transform: scale(0.62); opacity: 0.0; }}
}}

@keyframes et-login-orb-r {{
  0% {{ transform: translate3d(0, 0, 0) scale(0.34); opacity: 0.0; }}
  18% {{ transform: translate3d(0, 0, 0) scale(0.66); opacity: 0.34; }}
  78% {{ transform: translate3d(260px, 152px, 0) scale(1.28); opacity: 0.28; }}
  100% {{ transform: translate3d(300px, 190px, 0) scale(1.36); opacity: 0.0; }}
}}

@keyframes et-login-orb-b {{
  0% {{ transform: translate3d(0, 0, 0) scale(0.34); opacity: 0.0; }}
  18% {{ transform: translate3d(0, 0, 0) scale(0.66); opacity: 0.34; }}
  78% {{ transform: translate3d(-270px, 140px, 0) scale(1.28); opacity: 0.28; }}
  100% {{ transform: translate3d(-308px, 176px, 0) scale(1.36); opacity: 0.0; }}
}}

@keyframes et-login-orb-g {{
  0% {{ transform: translate3d(0, 0, 0) scale(0.34); opacity: 0.0; }}
  18% {{ transform: translate3d(0, 0, 0) scale(0.66); opacity: 0.34; }}
  78% {{ transform: translate3d(0px, -302px, 0) scale(1.28); opacity: 0.28; }}
  100% {{ transform: translate3d(0px, -344px, 0) scale(1.36); opacity: 0.0; }}
}}

@media (max-width: 900px) {{
  .et-galaxy-blob {{
    width: min(68vw, 380px);
  }}
  .et-login-intro-core {{
    width: 96px;
    height: 96px;
    margin-left: -48px;
    margin-top: -48px;
  }}
  .et-login-orb {{
    width: 132px;
    height: 132px;
    margin-left: -66px;
    margin-top: -66px;
  }}
}}

@media (prefers-reduced-motion: reduce) {{
  .et-galaxy-blob,
  .et-login-intro,
  .et-login-intro-core,
  .et-login-orb,
  .et-star-rain-overlay,
  .et-star-rain-core,
  .et-star-rain-dot,
  .et-login-star-dot,
  .et-login-starstorm,
  .et-login-star-core {{
    animation: none !important;
  }}
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

/* Diagnostico premium fijo */
.et-premium-diag {{
  background:
    linear-gradient(180deg, rgba(245,194,107,0.08), rgba(22,26,35,0.96)),
    {COL_CARD};
  border: 1px solid rgba(245,194,107,0.34);
  border-left: 4px solid rgba(245,194,107,0.92);
  border-radius: 16px;
  padding: 14px 16px;
  margin: 10px 0 14px 0;
  box-shadow: 0 10px 24px rgba(0,0,0,0.28);
}}

.et-premium-grid {{
  display: grid;
  gap: 8px;
}}

.et-premium-row {{
  font-size: 0.92rem;
  line-height: 1.35;
}}

.et-premium-label {{
  color: #9AA1B2;
  font-weight: 600;
  margin-right: 6px;
}}

.et-premium-value {{
  color: #E6E8EE;
  font-weight: 600;
}}

.et-premium-value.is-active {{
  color: #F5C26B;
}}

.et-premium-value.is-inactive {{
  color: #7FA7D8;
}}

.et-premium-value.is-risk {{
  color: #E26D5A;
}}

.et-premium-subtitle {{
  margin-top: 4px;
  color: #C8CFDA;
  font-size: 0.9rem;
  font-weight: 700;
}}

.et-premium-list {{
  margin: 4px 0 0 18px;
  padding: 0;
  color: #E6E8EE;
  font-size: 0.9rem;
}}

.et-premium-list li {{
  margin: 0 0 4px 0;
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
  top: 86px;           /* ajusta si tu header ocupa más/menos */
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

/* Responsivo: en pantallas pequeñas lo pegamos abajo para no tapar todo */
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

/* ===== Guía rápida inicial (modal) ===== */
.et-guide-backdrop {{
  position: fixed;
  inset: 0;
  background: rgba(8, 10, 16, 0.58);
  z-index: 10018;
}}

/* ===== Boton flotante guia ===== */
.st-key-quick_guide_fab {{
  position: fixed !important;
  right: 18px !important;
  left: unset !important;
  bottom: 18px !important;
  top: unset !important;
  width: fit-content !important;
  z-index: 10017 !important;
}}

.st-key-quick_guide_fab > div {{
  width: fit-content !important;
}}

.st-key-quick_guide_fab .stButton > button {{
  border-radius: 999px;
  border: 1px solid rgba(255,255,255,0.22);
  background: rgba(17, 24, 39, 0.92);
  color: #E6E8EE;
  font-weight: 700;
  padding: 0.5rem 0.95rem;
  box-shadow: 0 10px 24px rgba(0,0,0,0.42);
}}

.st-key-quick_guide_fab .stButton > button:hover {{
  border-color: rgba(245,194,107,0.55);
  color: #F5C26B;
}}

/* ===== Boton flotante actualizar (junto a guia) ===== */
.st-key-refresh_fab {{
  position: fixed !important;
  right: 176px !important;
  left: unset !important;
  bottom: 18px !important;
  top: unset !important;
  width: fit-content !important;
  z-index: 10017 !important;
}}

.st-key-refresh_fab > div {{
  width: fit-content !important;
}}

.st-key-refresh_fab .stButton > button {{
  border-radius: 999px;
  border: 1px solid #5BE36A;
  background: linear-gradient(135deg, #7CFF8A 0%, #39D353 100%);
  color: #0A2610;
  font-weight: 700;
  padding: 0.5rem 0.95rem;
  box-shadow: 0 8px 22px rgba(57, 211, 83, 0.45);
}}

.st-key-refresh_fab .stButton > button:hover {{
  filter: brightness(1.04);
  border-color: #A4FFAF;
}}

/* ===== Badge estado Live/Fallback ===== */
.st-key-live_status_badge {{
  position: fixed !important;
  right: 18px !important;
  bottom: 66px !important;
  top: unset !important;
  left: unset !important;
  width: fit-content !important;
  z-index: 10016 !important;
}}

.st-key-live_status_badge > div {{
  width: fit-content !important;
}}

.et-live-pill {{
  display: inline-flex;
  align-items: center;
  gap: 8px;
  border-radius: 999px;
  padding: 0.43rem 0.78rem;
  background: rgba(17, 24, 39, 0.92);
  border: 1px solid rgba(255,255,255,0.18);
  color: #E6E8EE;
  font-weight: 700;
  font-size: 0.82rem;
  box-shadow: 0 8px 20px rgba(0,0,0,0.35);
}}

.et-live-pill .dot {{
  width: 9px;
  height: 9px;
  border-radius: 50%;
}}

.et-live-pill.is-live .dot {{
  background: #39D353;
  box-shadow: 0 0 8px rgba(57,211,83,0.7);
}}

.et-live-pill.is-fallback .dot {{
  background: #F5C26B;
  box-shadow: 0 0 8px rgba(245,194,107,0.55);
}}

.et-live-pill .meta {{
  font-weight: 500;
  opacity: 0.9;
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
  .st-key-quick_guide_fab {{
    right: 12px !important;
    left: unset !important;
    bottom: 12px !important;
    top: unset !important;
  }}
  .st-key-refresh_fab {{
    right: 12px !important;
    bottom: 56px !important;
  }}
  .st-key-live_status_badge {{
    right: 12px !important;
    bottom: 102px !important;
  }}
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

# ============================================================
# BLOQUE: HELPERS DE PRESENTACION Y TEXTO
# ============================================================
# ========= [V1.2-B HELPERS - INICIO] =========
def color_por_decision(decision: str) -> str:
    d = (decision or "").upper().strip()
    if "MERCADO CERRADO" in d:
        return COL_TEXT
    if "NO OPERAR" in d or "RIESGO" in d:
        return COL_RED
    if "OPERAR" in d:
        return COL_GOLD
    if "OBSERVAR" in d or "ESPERAR" in d:
        return COL_BLUE
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

    fixes = {
        "Ã°Å¸â€Âµ": "🔵",
        "Ã°Å¸â€Â´": "🔴",
        "Ã°Å¸Å¸á": "🟡",
        "Ã°Å¸Â§Â ": "🧠",
        "Ã°Å¸Â§Â?": "🧠",
        "ðŸ§?": "🧠",
        "Ã‚¿": "¿",
        "ðŸ‘€": "👀",
        "ðŸ“£": "📣",
        "ðŸŸ¡": "🟡",
        "ðŸ”µ": "🔵",
        "ðŸ”´": "🔴",
        "Ã¡": "á",
        "Ã©": "é",
        "Ã­": "í",
        "Ã³": "ó",
        "Ãº": "ú",
        "Ã±": "ñ",
        "Ã": "Á",
        "Ã‰": "É",
        "Ã": "Í",
        "Ã“": "Ó",
        "Ãš": "Ú",
        "Ã‘": "Ñ",
        "Ã¢â‚¬â€": "—",
        "Ã¢â‚¬Å“": "“",
        "Ã¢â‚¬Â": "”",
        "â€“": "–",
        "â€¢": "•",
        "âš ï¸": "⚠️",
        "Ã¢ÅáÂ Ã¯Â¸Â": "⚠️",
        "Ã¢ÂÂ¸Ã¯Â¸Â": "⏸️",
        "Gu?a": "Guía",
        "gu?a": "guía",
        "r?pida": "rápida",
        "d?as": "días",
        "Habr?": "Habrá",
        "Aqu?": "Aquí",
        "aqu?": "aquí",
        "Cercan?a": "Cercanía",
        "sesi?n": "sesión",
        "vac?o": "vacío",
        "l?nea": "línea",
        "m?nimas": "mínimas",
        "m?nimo": "mínimo",
        "t?pico": "típico",
        "AN\x81LISIS": "ANÁLISIS",
        "N\x8cCLEO": "NÚCLEO",
        "GR\x81FICO": "GRÁFICO",
    }
    for bad, good in fixes.items():
        s = s.replace(bad, good)
    return s


def normalizar_objeto_ui(valor):
    if isinstance(valor, dict):
        return {k: normalizar_objeto_ui(v) for k, v in valor.items()}
    if isinstance(valor, list):
        return [normalizar_objeto_ui(v) for v in valor]
    if isinstance(valor, tuple):
        return tuple(normalizar_objeto_ui(v) for v in valor)
    if isinstance(valor, str):
        return normalizar_texto_ui(valor)
    return valor
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


def _star_burst_origin(panel_pos: str) -> tuple[float, float]:
    pos = (panel_pos or "").strip()
    mapping = {
        "Arriba derecha": (0.93, 0.26),
        "Arriba izquierda": (0.08, 0.26),
        "Abajo derecha": (0.93, 0.82),
        "Abajo izquierda": (0.08, 0.82),
    }
    return mapping.get(pos, (0.93, 0.26))


def _star_polygon_vertices(outer_r: float = 1.0, inner_r: float = 0.45, points: int = 5):
    vertices = []
    step = math.pi / points
    for i in range(points * 2):
        r = outer_r if i % 2 == 0 else inner_r
        ang = i * step - math.pi / 2
        vertices.append((r * math.cos(ang), r * math.sin(ang)))
    return vertices


def _point_in_polygon(x: float, y: float, poly) -> bool:
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


def _build_star_offsets(total: int, radius_px: float = 165.0, seed: int | None = None):
    rng = random.Random(seed if seed is not None else int(time.time() * 1000))
    poly = _star_polygon_vertices()
    out = []
    while len(out) < max(1, total):
        x = rng.uniform(-1.1, 1.1)
        y = rng.uniform(-1.1, 1.1)
        if _point_in_polygon(x, y, poly):
            out.append((x * radius_px, y * radius_px))
    return out


def render_star_rain_overlay(origin_x: float, origin_y: float):
    ox = min(max(float(origin_x), 0.05), 0.95)
    oy = min(max(float(origin_y), 0.05), 0.95)
    seed = int(time.time() * 1000)
    rng = random.Random(seed)
    total = 520
    colors = ["#4A90E2", "#F5C26B", "#E26D5A", "#FFFFFF"]
    offsets = _build_star_offsets(total=total, radius_px=240.0, seed=seed + 17)
    stars = []
    for ex, ey in offsets:
        color = colors[rng.randrange(len(colors))]
        sx = rng.uniform(-58.0, 58.0)
        sy = rng.uniform(-48.0, 48.0)
        bx = ex * rng.uniform(0.06, 0.16)
        by = ey * rng.uniform(0.06, 0.16)
        delay = rng.uniform(0.0, 0.72)
        dur = rng.uniform(4.4, 6.0)
        size = rng.uniform(1.0, 3.8)
        style = (
            f"--x0:{ox * 100:.2f}vw;--y0:{oy * 100:.2f}vh;"
            f"--sx:{sx:.2f}vw;--sy:{sy:.2f}vh;"
            f"--ex:{ex:.1f}px;--ey:{ey:.1f}px;"
            f"--bx:{bx:.1f}px;--by:{by:.1f}px;"
            f"--d:{delay:.2f}s;--dur:{dur:.2f}s;"
            f"--s:{size:.2f}px;--c:{color};"
        )
        stars.append(f'<span class="et-star-rain-dot" style="{style}"></span>')

    st.markdown(
        f"""
        <div class="et-star-rain-overlay" aria-hidden="true">
          <div class="et-star-rain-core" style="--x0:{ox * 100:.2f}vw;--y0:{oy * 100:.2f}vh;"></div>
          {''.join(stars)}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_login_star_merge_fx():
    seed = int(time.time() * 1000)
    rng = random.Random(seed)
    colors = ["#4A90E2", "#F5C26B", "#E26D5A", "#FFFFFF"]
    total = 720
    offsets = _build_star_offsets(total=total, radius_px=265.0, seed=seed + 11)
    parts = []
    for ex, ey in offsets:
        c = colors[rng.randrange(len(colors))]
        sx = rng.uniform(-58.0, 58.0)
        sy = rng.uniform(-48.0, 48.0)
        bx = ex * rng.uniform(0.05, 0.14)
        by = ey * rng.uniform(0.05, 0.14)
        delay = rng.uniform(0.0, 0.9)
        dur = rng.uniform(5.0, 6.8)
        size = rng.uniform(1.0, 3.2)
        style = (
            f"--sx:{sx:.2f}vw;--sy:{sy:.2f}vh;"
            f"--ex:{ex:.1f}px;--ey:{ey:.1f}px;"
            f"--bx:{bx:.1f}px;--by:{by:.1f}px;"
            f"--d:{delay:.2f}s;--dur:{dur:.2f}s;"
            f"--s:{size:.2f}px;--c:{c};"
        )
        parts.append(f'<span class="et-login-star-dot" style="{style}"></span>')

    st.markdown(
        f"""
        <div class="et-login-starstorm" aria-hidden="true">
          <div class="et-login-star-core"></div>
          {''.join(parts)}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# BLOQUE: IDLE MODE (PANTALLA EN REPOSO)
# ============================================================
IDLE_TIMEOUT_SEC = max(15, int(os.getenv("IDLE_TIMEOUT_SEC", "90")))


def _leer_idle_query_flag() -> bool:
    raw = "0"
    try:
        raw = str(st.query_params.get("idle", "0"))
    except Exception:
        try:
            raw = str((st.experimental_get_query_params().get("idle") or ["0"])[0])
        except Exception:
            raw = "0"
    return raw.strip().lower() in {"1", "true", "on", "yes"}


def _render_idle_watchdog(idle_mode: bool, timeout_sec: int):
    idle_js = "true" if idle_mode else "false"
    timeout_safe = max(15, int(timeout_sec))
    components.html(
        f"""
        <script>
        (function() {{
          const p = window.parent;
          if (!p || !p.location) return;

          const timeoutMs = {timeout_safe} * 1000;
          const idleMode = {idle_js};
          const state = p.__etIdleWatchdog || {{}};
          state.timeoutMs = timeoutMs;
          state.idleMode = idleMode;

          function setIdleFlag(v) {{
            try {{
              const url = new URL(p.location.href);
              const current = (url.searchParams.get("idle") || "0").toLowerCase();
              if (current === v) return;
              url.searchParams.set("idle", v);
              p.location.href = url.toString();
            }} catch (e) {{}}
          }}

          function onActivity() {{
            if (state.idleMode) {{
              setIdleFlag("0");
              return;
            }}
            if (state.timer) clearTimeout(state.timer);
            state.timer = setTimeout(function() {{
              setIdleFlag("1");
            }}, state.timeoutMs);
          }}

          if (!state.bound) {{
            ["mousemove", "mousedown", "keydown", "scroll", "touchstart", "pointerdown"].forEach(function(evt) {{
              p.addEventListener(evt, onActivity, {{ passive: true }});
            }});
            state.bound = true;
          }}

          if (state.idleMode && state.timer) {{
            clearTimeout(state.timer);
            state.timer = null;
          }}

          if (!state.idleMode && !state.timer) {{
            state.timer = setTimeout(function() {{
              setIdleFlag("1");
            }}, state.timeoutMs);
          }}

          p.__etIdleWatchdog = state;
        }})();
        </script>
        """,
        height=0,
        width=0,
    )


def _color_desde_esfera(esfera_txt: str) -> str:
    esfera_txt = (esfera_txt or "").lower()
    if "roj" in esfera_txt:
        return "rojo"
    if "dorad" in esfera_txt or "amarill" in esfera_txt:
        return "dorado"
    return "azul"


# ============================================================
# BLOQUE: AUTH - CONFIGURACION Y CONSTANTES
# ============================================================
USERS_DB_PATH = os.getenv("USERS_DB_PATH", os.path.join(os.path.dirname(__file__), "usuarios_db.json"))
SCANNER_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "scanner_config.json")
SCANNER_STATE_PATH = os.path.join(os.path.dirname(__file__), "scanner_state.json")
SCANNER_HEALTH_PATH = os.path.join(os.path.dirname(__file__), "scanner_health.json")
AUTH_COOKIE_PREFIX = os.getenv("AUTH_COOKIE_PREFIX", "estrella_trader")
# Cambia esta clave en producción con variable de entorno AUTH_COOKIE_PASSWORD.
AUTH_COOKIE_PASSWORD = os.getenv("AUTH_COOKIE_PASSWORD", "").strip()
if not AUTH_COOKIE_PASSWORD:
    seed = f"{os.path.abspath(os.path.dirname(__file__))}|estrella-auth-stable-fallback"
    AUTH_COOKIE_PASSWORD = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    logger.warning(
        "AUTH_COOKIE_PASSWORD no configurado; usando fallback estable local. "
        "Configuralo en produccion para sesiones persistentes y seguras."
    )
AUTH_COOKIE_USER_KEY = "uid"
AUTH_QUERY_SESSION_KEY = "et_session"
PREMIUM_ACCESS_CODE = os.getenv("PREMIUM_ACCESS_CODE", "").strip()


# ============================================================
# BLOQUE: AUTH - HELPERS DE TIEMPO, EMAIL Y SMTP
# ============================================================
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


def _normalizar_telegram_chat_id(chat_id: str) -> str:
    return str(chat_id or "").strip()


def _chat_id_telegram_valido(chat_id: str) -> bool:
    cid = _normalizar_telegram_chat_id(chat_id)
    if not cid:
        return True
    if cid.startswith("-"):
        return cid[1:].isdigit()
    return cid.isdigit()


def _normalizar_telegram_bot_username(username: str) -> str:
    return str(username or "").strip().lstrip("@")


def _bot_username_telegram_valido(username: str) -> bool:
    u = _normalizar_telegram_bot_username(username)
    if not u:
        return False
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
    return all(ch in allowed for ch in u)


def _leer_bot_username_telegram_config() -> str:
    try:
        with open(SCANNER_CONFIG_PATH, "r", encoding="utf-8-sig") as f:
            cfg = json.load(f)
        if not isinstance(cfg, dict):
            return ""
        notif = cfg.get("notification", {})
        if not isinstance(notif, dict):
            return ""
        tg = notif.get("telegram", {})
        if not isinstance(tg, dict):
            return ""
        return _normalizar_telegram_bot_username(tg.get("bot_username", ""))
    except Exception:
        return ""


def _guardar_bot_username_telegram_config(bot_username: str):
    u = _normalizar_telegram_bot_username(bot_username)
    if u and not _bot_username_telegram_valido(u):
        return False, "Username de bot invalido. Usa solo letras, numeros o guion bajo."

    cfg = {}
    try:
        if os.path.exists(SCANNER_CONFIG_PATH):
            with open(SCANNER_CONFIG_PATH, "r", encoding="utf-8-sig") as f:
                payload = json.load(f)
            if isinstance(payload, dict):
                cfg = payload
    except Exception:
        cfg = {}

    notif = cfg.get("notification")
    if not isinstance(notif, dict):
        notif = {}
        cfg["notification"] = notif
    tg = notif.get("telegram")
    if not isinstance(tg, dict):
        tg = {}
        notif["telegram"] = tg
    tg["bot_username"] = u

    try:
        with open(SCANNER_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        if u:
            return True, "Username del bot guardado."
        return True, "Username del bot eliminado."
    except Exception as exc:
        return False, f"No se pudo guardar scanner_config.json: {exc}"


def _safe_float_num(value, default=0.0) -> float:
    try:
        val = float(value)
        if pd.isna(val):
            return default
        return val
    except Exception:
        return default


def _leer_scanner_state() -> dict:
    try:
        if not os.path.exists(SCANNER_STATE_PATH):
            return {}
        with open(SCANNER_STATE_PATH, "r", encoding="utf-8-sig") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            return payload
    except Exception:
        return {}
    return {}


def _leer_scanner_health() -> dict:
    try:
        if not os.path.exists(SCANNER_HEALTH_PATH):
            return {}
        with open(SCANNER_HEALTH_PATH, "r", encoding="utf-8-sig") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            return payload
    except Exception:
        return {}
    return {}


def _parse_scanner_record_key(record_key: str, record: dict) -> tuple[str, str, str, str]:
    parts = [str(p).strip() for p in str(record_key or "").split("|")]
    market = parts[0] if len(parts) >= 1 else str(record.get("market", "")).strip()
    label = parts[1] if len(parts) >= 2 else str(record.get("label", "")).strip()
    ticker = parts[2] if len(parts) >= 3 else str(record.get("ticker", "")).strip()
    timeframe = parts[3] if len(parts) >= 4 else ""
    if not timeframe:
        timeframe = str(record.get("scan_target") or record.get("analysis_interval") or "15m").strip()
    return market, label, ticker, timeframe


def _rr_promedio_record(record: dict) -> tuple[float, int]:
    stats = record.get("quality_stats", {})
    if not isinstance(stats, dict):
        stats = {}
    rr_avg = _safe_float_num(stats.get("rr_avg"), 0.0)
    rr_samples = int(stats.get("rr_samples", 0) or 0)
    if rr_avg > 0 and rr_samples > 0:
        return rr_avg, rr_samples

    rr_values = []
    history = record.get("quality_history", [])
    if isinstance(history, list):
        for event in history:
            if not isinstance(event, dict):
                continue
            status = str(event.get("status", "")).strip().lower()
            if status not in {"win", "loss", "timeout"}:
                continue
            rr = _safe_float_num(event.get("rr_estimado"), 0.0)
            if rr > 0:
                rr_values.append(rr)
    if not rr_values:
        return rr_avg, rr_samples
    return sum(rr_values) / len(rr_values), len(rr_values)


def _score_precision_operable(accuracy_pct: float, rr_avg: float, timeout_pct: float, resolved: int) -> float:
    sample_factor = min(1.0, max(0.0, resolved) / 25.0)
    base = (accuracy_pct * 0.72) + (min(rr_avg, 3.0) * 18.0) - (timeout_pct * 0.25)
    score = base * (0.55 + 0.45 * sample_factor)
    return round(max(0.0, min(100.0, score)), 2)


def _scanner_precision_panel_data():
    state = _leer_scanner_state()
    symbols = state.get("symbols", {})
    if not isinstance(symbols, dict):
        return pd.DataFrame(), pd.DataFrame(), {
            "resolved": 0,
            "wins": 0,
            "losses": 0,
            "timeouts": 0,
            "accuracy_pct": 0.0,
            "rr_avg": 0.0,
            "recommend_count": 0,
            "last_checked_utc": "",
        }

    has_target_keys = any(len(str(k).split("|")) >= 4 for k in symbols.keys())
    rows = []
    ranking_acc = {}
    summary = {
        "resolved": 0,
        "wins": 0,
        "losses": 0,
        "timeouts": 0,
        "rr_weighted_sum": 0.0,
        "rr_weighted_count": 0,
        "last_checked_utc": "",
    }

    for record_key, record in symbols.items():
        if not isinstance(record, dict):
            continue
        if has_target_keys and len(str(record_key).split("|")) < 4 and not str(record.get("scan_target", "")).strip():
            continue

        market, label, ticker, timeframe = _parse_scanner_record_key(record_key, record)
        stats = record.get("quality_stats", {})
        if not isinstance(stats, dict):
            stats = {}

        wins = int(stats.get("wins", 0) or 0)
        losses = int(stats.get("losses", 0) or 0)
        timeouts = int(stats.get("timeouts", 0) or 0)
        resolved = int(stats.get("resolved", wins + losses + timeouts) or (wins + losses + timeouts))
        accuracy_pct = _safe_float_num(stats.get("accuracy_pct"), 0.0)
        if resolved > 0 and accuracy_pct <= 0:
            accuracy_pct = wins / resolved * 100.0
        timeout_pct = _safe_float_num(stats.get("timeout_pct"), 0.0)
        if resolved > 0 and timeout_pct <= 0:
            timeout_pct = timeouts / resolved * 100.0
        rr_avg, rr_samples = _rr_promedio_record(record)
        score = _score_precision_operable(accuracy_pct, rr_avg, timeout_pct, resolved)

        calibration = record.get("quality_calibration", {})
        if not isinstance(calibration, dict):
            calibration = {}
        thresholds = record.get("effective_thresholds", {})
        if not isinstance(thresholds, dict):
            thresholds = {}
        cal_mode = str(calibration.get("mode", "n/a")).strip() or "n/a"
        cal_scope = str(calibration.get("scope", "")).strip()
        cal_label = f"{cal_scope}:{cal_mode}" if cal_scope else cal_mode

        rows.append(
            {
                "Mercado": market,
                "Activo": label,
                "Ticker": ticker,
                "Timeframe": timeframe,
                "W": wins,
                "L": losses,
                "Timeout": timeouts,
                "Resueltas": resolved,
                "Acierto %": round(accuracy_pct, 2),
                "RR prom.": round(rr_avg, 3),
                "Calibracion": cal_label,
                "Conf min": int(_safe_float_num(thresholds.get("min_confidence_effective"), 0)),
                "RR min": round(_safe_float_num(thresholds.get("min_rr"), 0.0), 2),
                "MTF min": int(_safe_float_num(thresholds.get("min_mtf_confirmations"), 0)),
                "Score operativo": score,
            }
        )

        summary["wins"] += wins
        summary["losses"] += losses
        summary["timeouts"] += timeouts
        summary["resolved"] += resolved
        if rr_samples > 0 and rr_avg > 0:
            summary["rr_weighted_sum"] += rr_avg * rr_samples
            summary["rr_weighted_count"] += rr_samples

        last_checked = str(record.get("last_checked_utc", "")).strip()
        if last_checked and last_checked > summary["last_checked_utc"]:
            summary["last_checked_utc"] = last_checked

        agg_key = f"{market}|{label}|{ticker}"
        agg = ranking_acc.get(agg_key)
        if agg is None:
            agg = {
                "Mercado": market,
                "Activo": label,
                "Ticker": ticker,
                "wins": 0,
                "losses": 0,
                "timeouts": 0,
                "resolved": 0,
                "rr_sum": 0.0,
                "rr_count": 0,
                "best_timeframe": timeframe,
                "best_score": score,
            }
            ranking_acc[agg_key] = agg

        agg["wins"] += wins
        agg["losses"] += losses
        agg["timeouts"] += timeouts
        agg["resolved"] += resolved
        if rr_samples > 0 and rr_avg > 0:
            agg["rr_sum"] += rr_avg * rr_samples
            agg["rr_count"] += rr_samples
        if score >= agg["best_score"]:
            agg["best_score"] = score
            agg["best_timeframe"] = timeframe

    metrics_df = pd.DataFrame(rows)
    if not metrics_df.empty:
        metrics_df = metrics_df.sort_values(
            by=["Score operativo", "Acierto %", "Resueltas"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

    ranking_rows = []
    for agg in ranking_acc.values():
        resolved = int(agg.get("resolved", 0) or 0)
        wins = int(agg.get("wins", 0) or 0)
        losses = int(agg.get("losses", 0) or 0)
        timeouts = int(agg.get("timeouts", 0) or 0)
        accuracy_pct = (wins / resolved * 100.0) if resolved > 0 else 0.0
        timeout_pct = (timeouts / resolved * 100.0) if resolved > 0 else 0.0
        rr_avg = (agg["rr_sum"] / agg["rr_count"]) if agg["rr_count"] > 0 else 0.0
        score = _score_precision_operable(accuracy_pct, rr_avg, timeout_pct, resolved)
        conviene = "SI" if resolved >= 8 and accuracy_pct >= 55.0 and rr_avg >= 1.5 and timeout_pct <= 35.0 else "NO"
        ranking_rows.append(
            {
                "Mercado": agg["Mercado"],
                "Activo": agg["Activo"],
                "Ticker": agg["Ticker"],
                "Mejor timeframe": agg.get("best_timeframe", ""),
                "Resueltas": resolved,
                "Acierto %": round(accuracy_pct, 2),
                "RR prom.": round(rr_avg, 3),
                "Timeout %": round(timeout_pct, 2),
                "Score operativo": score,
                "Conviene operar": conviene,
            }
        )

    ranking_df = pd.DataFrame(ranking_rows)
    if not ranking_df.empty:
        ranking_df = ranking_df.sort_values(
            by=["Score operativo", "Acierto %", "Resueltas"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

    summary["accuracy_pct"] = round(
        (summary["wins"] / summary["resolved"] * 100.0) if summary["resolved"] > 0 else 0.0,
        2,
    )
    summary["rr_avg"] = round(
        (summary["rr_weighted_sum"] / summary["rr_weighted_count"]) if summary["rr_weighted_count"] > 0 else 0.0,
        3,
    )
    summary["recommend_count"] = int((ranking_df["Conviene operar"] == "SI").sum()) if not ranking_df.empty else 0
    summary.pop("rr_weighted_sum", None)
    summary.pop("rr_weighted_count", None)
    return metrics_df, ranking_df, summary


def _telegram_bot_username_actual() -> str:
    env_user = _normalizar_telegram_bot_username(os.getenv("TELEGRAM_BOT_USERNAME", ""))
    if env_user:
        return env_user
    return _leer_bot_username_telegram_config()


def _telegram_bot_url() -> str:
    bot_username = _telegram_bot_username_actual()
    if bot_username:
        return f"https://t.me/{bot_username}"
    return "https://t.me"


def _telegram_start_code_usuario(user_id: str) -> str:
    uid = str(user_id or "").strip().lower()
    if not uid:
        return ""
    raw = f"{uid}|{AUTH_COOKIE_PASSWORD}|telegram-link"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"et{digest[:16]}"


def _telegram_bot_start_url(user_id: str = "") -> str:
    bot_username = _telegram_bot_username_actual()
    if not bot_username:
        return "https://t.me"
    base = f"https://t.me/{bot_username}"
    start_code = _telegram_start_code_usuario(user_id)
    if not start_code:
        return base
    return f"{base}?start={start_code}"


def _extraer_chat_id_update_por_start(update: dict, start_code: str) -> str:
    if not isinstance(update, dict):
        return ""
    expected = str(start_code or "").strip().lower()
    if not expected:
        return ""

    for key in ("message", "edited_message", "channel_post", "edited_channel_post"):
        payload = update.get(key, {})
        if not isinstance(payload, dict):
            continue
        chat = payload.get("chat", {})
        if not isinstance(chat, dict):
            continue

        text = str(payload.get("text", "") or payload.get("caption", "")).strip().lower()
        if not text:
            continue

        match = False
        if text.startswith("/start"):
            parts = text.split()
            if len(parts) >= 2 and parts[1].strip() == expected:
                match = True
        if expected in text:
            match = True

        if not match:
            continue

        chat_id = _normalizar_telegram_chat_id(chat.get("id", ""))
        if _chat_id_telegram_valido(chat_id):
            return chat_id
    return ""


def _detectar_chat_id_telegram_desde_start(user_id: str):
    start_code = _telegram_start_code_usuario(user_id)
    if not start_code:
        return False, "Usuario invalido para conectar Telegram.", ""

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        return False, "TELEGRAM_BOT_TOKEN no esta configurado en el servidor.", ""

    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{token}/getUpdates",
            params={"timeout": 0},
            timeout=20,
        )
    except Exception as exc:
        return False, f"No se pudo consultar Telegram: {exc}", ""

    if resp.status_code >= 400:
        return False, f"Telegram getUpdates HTTP {resp.status_code}.", ""

    try:
        payload = resp.json()
    except Exception:
        return False, "Respuesta invalida de Telegram.", ""

    if not payload.get("ok", False):
        return False, payload.get("description", "Error Telegram sin descripcion."), ""

    updates = payload.get("result", [])
    if not isinstance(updates, list):
        updates = []

    best_update_id = -1
    best_chat_id = ""
    for upd in updates:
        chat_id = _extraer_chat_id_update_por_start(upd, start_code)
        if not chat_id:
            continue
        try:
            upd_id = int(upd.get("update_id", 0) or 0)
        except Exception:
            upd_id = 0
        if upd_id >= best_update_id:
            best_update_id = upd_id
            best_chat_id = chat_id

    if not best_chat_id:
        return False, "No encontramos tu chat. Abre el bot, toca Iniciar y vuelve a intentar.", ""
    return True, "", best_chat_id


def conectar_telegram_usuario_desde_bot(user_id: str):
    ok, msg, detected_chat_id = _detectar_chat_id_telegram_desde_start(user_id)
    if not ok:
        return False, msg, None
    return actualizar_telegram_usuario(user_id, detected_chat_id)


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


def _candidate_password_hashes(password: str) -> list[str]:
    raw = password or ""
    hashes: list[str] = []
    seen: set[str] = set()
    for variant in (raw, raw.strip()):
        if variant in seen:
            continue
        seen.add(variant)
        hashes.append(_hash_password(variant))
    return hashes


# ============================================================
# BLOQUE: AUTH - PERSISTENCIA DE USUARIOS Y COOKIES
# ============================================================
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


def _cookie_manager():
    if not _HAS_COOKIE_MANAGER:
        return None
    if "_auth_cookie_manager" not in st.session_state:
        st.session_state["_auth_cookie_manager"] = EncryptedCookieManager(
            prefix=f"{AUTH_COOKIE_PREFIX}_",
            password=AUTH_COOKIE_PASSWORD,
        )
    return st.session_state["_auth_cookie_manager"]


def _cookie_manager_ready(cookie_mgr) -> bool:
    if cookie_mgr is None:
        return False
    try:
        return bool(cookie_mgr.ready())
    except Exception:
        return False


def _leer_cookie_uid():
    cookie_mgr = _cookie_manager()
    if not _cookie_manager_ready(cookie_mgr):
        return None
    try:
        uid = str(cookie_mgr.get(AUTH_COOKIE_USER_KEY) or "").strip()
    except Exception:
        return None
    return uid or None


def _escribir_cookie_uid(uid: str) -> bool:
    cookie_mgr = _cookie_manager()
    if not _cookie_manager_ready(cookie_mgr):
        return False
    uid = str(uid or "").strip()
    try:
        if uid:
            cookie_mgr[AUTH_COOKIE_USER_KEY] = uid
        else:
            try:
                del cookie_mgr[AUTH_COOKIE_USER_KEY]
            except Exception:
                pass
        cookie_mgr.save()
        return True
    except Exception:
        return False


def _escribir_cookie_uid_retry(uid: str, retries: int = 6, delay_sec: float = 0.08) -> bool:
    for _ in range(max(1, retries)):
        if _escribir_cookie_uid(uid):
            return True
        time.sleep(delay_sec)
    return False


def _firmar_session_uid(uid: str) -> str:
    raw_uid = str(uid or "").strip()
    if not raw_uid:
        return ""
    digest = hmac.new(
        AUTH_COOKIE_PASSWORD.encode("utf-8"),
        raw_uid.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return digest[:32]


def _crear_session_token(uid: str) -> str:
    raw_uid = str(uid or "").strip()
    if not raw_uid:
        return ""
    signature = _firmar_session_uid(raw_uid)
    if not signature:
        return ""
    return f"{raw_uid}.{signature}"


def _validar_session_token(token: str) -> str:
    raw = str(token or "").strip()
    if not raw or "." not in raw:
        return ""
    uid, signature = raw.rsplit(".", 1)
    uid = str(uid or "").strip()
    signature = str(signature or "").strip().lower()
    if not uid or not signature:
        return ""
    expected = _firmar_session_uid(uid).lower()
    if not expected:
        return ""
    if hmac.compare_digest(signature, expected):
        return uid
    return ""


def _leer_query_param_text(key: str) -> str:
    try:
        raw = st.query_params.get(key, "")
        if isinstance(raw, list):
            raw = raw[-1] if raw else ""
        return str(raw or "").strip()
    except Exception:
        try:
            raw = (st.experimental_get_query_params().get(key) or [""])[-1]
            return str(raw or "").strip()
        except Exception:
            return ""


def _escribir_query_session_token(uid: str) -> None:
    token = _crear_session_token(uid)
    current_token = _leer_query_param_text(AUTH_QUERY_SESSION_KEY)
    if token == current_token:
        return

    idle_raw = _leer_query_param_text("idle")
    params: dict[str, str] = {}
    if idle_raw:
        params["idle"] = idle_raw
    if token:
        params[AUTH_QUERY_SESSION_KEY] = token

    try:
        st.experimental_set_query_params(**params)
    except Exception:
        try:
            qp = st.query_params
            for key in list(qp.keys()):
                if key not in params:
                    del qp[key]
            for key, value in params.items():
                qp[key] = value
        except Exception:
            pass


def _leer_uid_desde_session_token() -> str:
    token = _leer_query_param_text(AUTH_QUERY_SESSION_KEY)
    if not token:
        return ""
    return _validar_session_token(token)


# ============================================================
# BLOQUE: AUTH - MODELO PUBLICO DE USUARIO
# ============================================================
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


def _quick_guide_seen(record: dict) -> bool:
    # Usuarios antiguos (sin este campo) no deben recibir auto-onboarding.
    if "quick_guide_seen" not in record:
        return True
    return bool(record.get("quick_guide_seen", True))


def _usuario_publico(record: dict) -> dict:
    activo = _premium_activo(record)
    return {
        "id": record.get("id"),
        "username": record.get("username", ""),
        "email": record.get("email", ""),
        "telegram_chat_id": _normalizar_telegram_chat_id(record.get("telegram_chat_id", "")),
        "es_premium": activo,
        "premium_until": record.get("premium_until"),
        "quick_guide_seen": _quick_guide_seen(record),
        "autenticado": True,
    }


def _usuario_invitado() -> dict:
    return {
        "id": "guest",
        "username": "Invitado",
        "email": "",
        "telegram_chat_id": "",
        "es_premium": False,
        "premium_until": None,
        "quick_guide_seen": True,
        "autenticado": False,
    }


# ============================================================
# BLOQUE: AUTH - OPERACIONES DE CUENTA Y PREMIUM
# ============================================================
def registrar_usuario(username: str, email: str, password: str, telegram_chat_id: str = ""):
    username = (username or "").strip()
    email = _normalizar_email(email)
    password = (password or "").strip()
    telegram_chat_id = _normalizar_telegram_chat_id(telegram_chat_id)

    if len(username) < 3:
        return False, "El nombre de usuario debe tener al menos 3 caracteres.", None
    if not _es_gmail(email):
        return False, "Usa un correo Gmail valido (ejemplo@gmail.com).", None
    if len(password) < 6:
        return False, "La contraseña debe tener al menos 6 caracteres.", None
    if not _chat_id_telegram_valido(telegram_chat_id):
        return False, "El Chat ID de Telegram debe ser numerico.", None

    db = _cargar_usuarios_db()
    if any(_normalizar_email(u.get("email")) == email for u in db["users"]):
        return False, "Ya existe una cuenta con ese correo.", None

    now_iso = _iso_utc(_utc_now())
    record = {
        "id": str(uuid4()),
        "username": username,
        "email": email,
        "telegram_chat_id": telegram_chat_id,
        "password_hash": _hash_password(password),
        "es_premium": False,
        "premium_until": None,
        "quick_guide_seen": False,
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
    password_hashes = _candidate_password_hashes(password)

    db = _cargar_usuarios_db()
    changed = False
    for u in db["users"]:
        if _normalizar_email(u.get("email")) != email:
            continue
        if str(u.get("password_hash") or "") not in password_hashes:
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


def guardar_sesion_local(user_id: str):
    uid = (user_id or "").strip()
    if not uid:
        limpiar_sesion_local()
        return
    st.session_state["auth_cookie_pending_uid"] = uid
    _sincronizar_cookie_pendiente()
    _escribir_query_session_token(uid)


def limpiar_sesion_local():
    st.session_state["auth_cookie_pending_uid"] = ""
    _sincronizar_cookie_pendiente()
    _escribir_query_session_token("")


def _sincronizar_cookie_pendiente():
    if "auth_cookie_pending_uid" not in st.session_state:
        return
    pending_uid = st.session_state.get("auth_cookie_pending_uid")
    if pending_uid is None:
        return
    if _escribir_cookie_uid_retry(pending_uid):
        st.session_state["auth_cookie_pending_uid"] = None


def recuperar_sesion_local():
    cookie_mgr = _cookie_manager()
    if _HAS_COOKIE_MANAGER and not _cookie_manager_ready(cookie_mgr):
        attempts = int(st.session_state.get("auth_cookie_boot_attempts", 0))
        st.session_state["auth_cookie_boot_attempts"] = attempts + 1
    else:
        st.session_state["auth_cookie_boot_attempts"] = 0

    _sincronizar_cookie_pendiente()
    uid = _leer_cookie_uid()
    if not uid:
        uid = _leer_uid_desde_session_token()
        if uid and _HAS_COOKIE_MANAGER:
            _escribir_cookie_uid_retry(uid)
    if not uid:
        return _usuario_invitado()

    user_public = recargar_usuario(uid)
    if not user_public.get("autenticado", False):
        limpiar_sesion_local()
    else:
        if _HAS_COOKIE_MANAGER:
            _escribir_cookie_uid_retry(uid)
        _escribir_query_session_token(uid)
    return user_public


def marcar_guia_rapida_vista(user_id: str):
    db = _cargar_usuarios_db()
    for u in db["users"]:
        if u.get("id") != user_id:
            continue
        if bool(u.get("quick_guide_seen", False)):
            return True, _usuario_publico(u)
        u["quick_guide_seen"] = True
        u["updated_at"] = _iso_utc(_utc_now())
        _guardar_usuarios_db(db)
        return True, _usuario_publico(u)
    return False, None


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


def activar_premium_por_codigo(user_id: str, codigo: str):
    if not PREMIUM_ACCESS_CODE:
        return False, "Activación por código deshabilitada. Configura PREMIUM_ACCESS_CODE en servidor.", None
    code = str(codigo or "").strip()
    if code != PREMIUM_ACCESS_CODE:
        return False, "Código inválido.", None
    ok, _, user_public = comprar_premium_usuario(user_id, 30)
    if not ok or not user_public:
        return False, "No se pudo activar Premium por código.", None
    return True, "Premium activado por código.", user_public


def actualizar_telegram_usuario(user_id: str, telegram_chat_id: str):
    chat_id = _normalizar_telegram_chat_id(telegram_chat_id)
    if not _chat_id_telegram_valido(chat_id):
        return False, "El Chat ID de Telegram debe ser numerico.", None

    db = _cargar_usuarios_db()
    for u in db["users"]:
        if u.get("id") != user_id:
            continue
        u["telegram_chat_id"] = chat_id
        u["updated_at"] = _iso_utc(_utc_now())
        _guardar_usuarios_db(db)
        if chat_id:
            return True, "Telegram actualizado.", _usuario_publico(u)
        return True, "Telegram eliminado de tu cuenta.", _usuario_publico(u)
    return False, "No se encontró el usuario.", None


# ============================================================
# BLOQUE: DATOS - NORMALIZACION HORARIA Y MAPAS
# ============================================================
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
    "30m": "30min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1day",
}

TD_SYMBOLS = {
    "Oro (XAU/USD)": "XAU/USD",
    "NASDAQ 100": "NDX",
    "S&P 500": "SPX",
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


# ============================================================
# BLOQUE: DATOS - FUENTES EXTERNAS Y HORARIO DE MERCADO
# ============================================================
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
    if mercado in ("Cripto", "Crypto"):
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


# ============================================================
# BLOQUE: CACHE - PROCESADO Y ESTADO DE ANALISIS
# ============================================================
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

# ============================================================
# BLOQUE: ESTADO EN MEMORIA (SESSION_STATE)
# ============================================================
if "memoria_estrella" not in st.session_state:
    st.session_state.memoria_estrella = []


# ============================================================
# BLOQUE: FILTRADO DE MEMORIA POR ESFERA
# ============================================================
def filtrar_por_esfera(recuerdos, esfera):
    return [r for r in recuerdos if r.get("esfera") == esfera]


# -----------------------
if "memoria_estrella" not in st.session_state:
    st.session_state.memoria_estrella = []


# ============================================================
# BLOQUE: RENDER 3D DE LA ESTRELLA (PLOTLY)
# ============================================================
def estrella_visual(
    color_lider,
    n_puntos=600,
    bubble_shell=True,
    all_lit=False,
    animate=True,
    size_px=350,
    camera_eye=None
):
    colores = {
        "dorado": "#F5C26B",
        "azul": "#4A90E2",
        "rojo": "#E26D5A"
    }

    rng = random.Random(42)
    if camera_eye is None:
        camera_eye = dict(x=1.4, y=1.2, z=0.8)

    t = time.time() if animate else 0.0
    pulse = 0.5 + 0.5 * math.sin(t * 2.0) if animate else 0.5
    angle = t * 0.2 if animate else 0.0

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

    def halton(index: int, base: int) -> float:
        f = 1.0
        r = 0.0
        i = index
        while i > 0:
            f /= base
            r += f * (i % base)
            i //= base
        return r

    star = star_vertices()
    palette = ["rojo", "azul", "dorado"]
    per_color_counts = {
        "rojo": 200,
        "azul": 500,
        "dorado": 500,
    }
    total_points = sum(per_color_counts.values())

    xs, ys, zs = [], [], []
    idx = 1
    while len(xs) < total_points:
        x = (halton(idx, 2) * 2.2) - 1.1
        y = (halton(idx, 3) * 2.2) - 1.1
        idx += 1
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

    colors = []
    sizes = []
    is_lider = []
    color_labels = []
    for color in palette:
        color_labels.extend([color] * per_color_counts[color])
    rng.shuffle(color_labels)

    for color in color_labels:
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
        width=size_px,
        height=size_px,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        scene=dict(
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            zaxis=dict(visible=False),
            camera_eye=camera_eye
        ),
        showlegend=False
    )

    return fig


esfera_visual = estrella_visual


# ============================================================
# BLOQUE: RENDER DE FONDO + FX DE LOGIN
# ============================================================
def render_estrella_fondo(color_lider: str, mercado_abierto_local: bool):
    fig_bg = esfera_visual(
        color_lider,
        n_puntos=1200,
        bubble_shell=True,
        all_lit=not mercado_abierto_local,
        animate=False,
        size_px=BG_STAR_SIZE_PX,
        camera_eye=dict(x=0.0, y=0.0, z=2.15),
    )
    fig_bg.update_layout(autosize=False)
    with st.container(key="bg_star_full"):
        st.plotly_chart(
            fig_bg,
            use_container_width=False,
            config={"displayModeBar": False, "responsive": False},
            key="bg_star_plotly",
        )


def render_login_galaxia_fx(usuario_actual: dict):
    if not bool((usuario_actual or {}).get("autenticado", False)):
        return

    st.markdown(
        """
        <div class="et-galaxy-ambient" aria-hidden="true">
          <div class="et-galaxy-blob et-galaxy-b1"></div>
          <div class="et-galaxy-blob et-galaxy-b2"></div>
          <div class="et-galaxy-blob et-galaxy-b3"></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if st.session_state.pop("play_login_intro", False):
        render_login_star_merge_fx()


st.markdown('<div class="et-core-watch" aria-hidden="true"></div>', unsafe_allow_html=True)

# Activa/desactiva modo reposo por inactividad solo para sesiones autenticadas.
# En Safari/iPhone el touchstart del watchdog puede competir con el login y forzar una recarga.
idle_mode = False
idle_watchdog_enabled = bool((st.session_state.get("usuario") or {}).get("autenticado", False))
if idle_watchdog_enabled:
    idle_mode = _leer_idle_query_flag()
    _render_idle_watchdog(idle_mode=idle_mode, timeout_sec=IDLE_TIMEOUT_SEC)

if idle_mode:
    prev_color = st.session_state.get("idle_star_color")
    if prev_color not in {"rojo", "azul", "dorado"}:
        payload_prev = st.session_state.get("star_payload") or {}
        estado_prev = payload_prev.get("estado", {}) if isinstance(payload_prev, dict) else {}
        prev_color = _color_desde_esfera(estado_prev.get("esfera", ""))
    if prev_color not in {"rojo", "azul", "dorado"}:
        prev_color = "azul"

    st.markdown(
        """
        <style>
        header[data-testid="stHeader"] { display: none !important; }
        [data-testid="stToolbar"] { display: none !important; }
        [data-testid="stSidebar"] { display: none !important; }
        .block-container { padding-top: 0 !important; padding-bottom: 0 !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    render_estrella_fondo(prev_color, mercado_abierto_local=True)
    st.stop()

# ====== CUENTA + PREMIUM (ANTES DEL ANÁLISIS) ======
# ============================================================
# BLOQUE: SIDEBAR DE CUENTA Y PREMIUM
# ============================================================
if "usuario" not in st.session_state:
    st.session_state["usuario"] = _usuario_invitado()

# Reintenta guardar cookie pendiente en cada ciclo (cuando el manager ya esté listo).
_sincronizar_cookie_pendiente()

# Reintenta recuperar sesión guardada (cookie) cuando aún está como invitado.
if not st.session_state["usuario"].get("autenticado", False):
    recovered_user = recuperar_sesion_local()
    if recovered_user.get("autenticado", False):
        st.session_state["usuario"] = recovered_user

# Sincroniza el estado premium/autenticación desde base local.
if st.session_state["usuario"].get("autenticado", False):
    st.session_state["usuario"] = recargar_usuario(st.session_state["usuario"]["id"])
    if st.session_state["usuario"].get("autenticado", False):
        guardar_sesion_local(st.session_state["usuario"]["id"])
    else:
        limpiar_sesion_local()

with st.sidebar.expander("Cuenta", expanded=True):
    usuario_ui = st.session_state["usuario"]
    if not _HAS_COOKIE_MANAGER:
        st.caption("Recordarme por navegador desactivado: falta streamlit-cookies-manager.")
    elif st.session_state.get("auth_cookie_boot_attempts", 0) >= 1:
        st.caption("Tu navegador puede estar bloqueando cookies; la sesión puede no persistir al refrescar.")

    if not usuario_ui.get("autenticado", False):
        st.caption("Inicia sesion o crea cuenta.")
        if hasattr(st, "link_button"):
            st.link_button("Abrir bot de Telegram", _telegram_bot_url(), use_container_width=True)
        else:
            st.markdown(f"[Abrir bot de Telegram]({_telegram_bot_url()})")
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
                    st.session_state["play_login_intro"] = True
                    guardar_sesion_local(user_public.get("id"))
                    st.success(msg)
                else:
                    st.error(msg)
        else:
            username_reg = st.text_input("Usuario", key="reg_user_v1")
            email_reg = st.text_input("Correo Gmail", placeholder="tuusuario@gmail.com", key="reg_email_v1")
            st.caption("Telegram se conecta despues con el boton Conectar Telegram.")
            pass_reg = st.text_input("Contraseña", type="password", key="reg_pass_v1")
            pass_reg2 = st.text_input("Confirmar contraseña", type="password", key="reg_pass2_v1")
            if st.button("Crear cuenta", key="btn_register_v1"):
                if pass_reg != pass_reg2:
                    st.error("Las contraseñas no coinciden.")
                else:
                    ok, msg, user_public = registrar_usuario(
                        username_reg,
                        email_reg,
                        pass_reg,
                        "",
                    )
                    if ok and user_public:
                        st.session_state["usuario"] = user_public
                        st.session_state["play_login_intro"] = True
                        guardar_sesion_local(user_public.get("id"))
                        st.success(msg)
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
        st.markdown("**Telegram**")
        chat_guardado = str(usuario_ui.get("telegram_chat_id", "") or "").strip()
        if chat_guardado:
            st.success("Telegram conectado.")
        else:
            st.info("Telegram no conectado.")
        st.caption("1) Abre el bot. 2) Toca Iniciar. 3) Pulsa Conectar Telegram aqui.")

        tg_start_url = _telegram_bot_start_url(usuario_ui.get("id", ""))
        col_tg_open, col_tg_connect, col_tg_clear = st.columns(3)
        if hasattr(st, "link_button"):
            col_tg_open.link_button("Abrir bot", tg_start_url, use_container_width=True)
        else:
            col_tg_open.markdown(f"[Abrir bot]({tg_start_url})")

        if col_tg_connect.button("Conectar Telegram", key="btn_connect_telegram_v1"):
            ok, msg, user_public = conectar_telegram_usuario_desde_bot(usuario_ui["id"])
            if ok and user_public:
                st.session_state["usuario"] = user_public
                st.success(msg)
                rerun_app()
            else:
                st.error(msg)

        if col_tg_clear.button("Desconectar", key="btn_clear_telegram_v1"):
            ok, msg, user_public = actualizar_telegram_usuario(usuario_ui["id"], "")
            if ok and user_public:
                st.session_state["usuario"] = user_public
                st.success(msg)
                rerun_app()
            else:
                st.error(msg)

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

        if PREMIUM_ACCESS_CODE:
            codigo_premium = st.text_input(
                "Código premium",
                type="password",
                key="premium_code_input_v1",
                placeholder="Ingresa tu código",
            )
            if st.button("Activar por código", key="btn_premium_code_v1"):
                ok, msg, user_public = activar_premium_por_codigo(usuario_ui["id"], codigo_premium)
                if ok and user_public:
                    st.session_state["usuario"] = user_public
                    st.success(msg)
                    rerun_app()
                else:
                    st.error(msg)
        else:
            st.caption("Activación por código deshabilitada en este entorno.")

        if st.button("Cerrar sesión", key="btn_logout_v1"):
            st.session_state["usuario"] = _usuario_invitado()
            st.session_state["play_login_intro"] = False
            limpiar_sesion_local()
            rerun_app()

# ============================================================
# BLOQUE: ESTADO DE USUARIO ACTUAL + FX POST-LOGIN
# ============================================================
usuario = st.session_state["usuario"]
if "play_login_intro" not in st.session_state:
    st.session_state["play_login_intro"] = False
render_login_galaxia_fx(usuario)
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
            "Mostrar diagnostico. Solo en Premium",
            value=st.session_state.get("debug_v13", False),
            help="Activa el panel de diagnostico avanzado."
        )

if "show_quick_guide" not in st.session_state:
    st.session_state["show_quick_guide"] = False
if "quick_guide_auto_open_user_id" not in st.session_state:
    st.session_state["quick_guide_auto_open_user_id"] = None

if (
    usuario.get("autenticado", False)
    and not bool(usuario.get("quick_guide_seen", True))
    and st.session_state.get("quick_guide_auto_open_user_id") != usuario.get("id")
):
    st.session_state["show_quick_guide"] = True
    st.session_state["quick_guide_auto_open_user_id"] = usuario.get("id")
    ok_guide_seen, user_public = marcar_guia_rapida_vista(usuario.get("id"))
    if ok_guide_seen and user_public:
        st.session_state["usuario"] = user_public
        usuario = user_public

with st.container(key="quick_guide_fab"):
    if st.button("📘 Guía rápida", key="btn_open_quick_guide_fab"):
        st.session_state["show_quick_guide"] = True
        rerun_app()

with st.container(key="refresh_fab"):
    if st.button("Actualizar lectura", key="btn_actualizar_lectura_fab"):
        st.session_state["star_force_refresh"] = True
        st.session_state["play_star_rain"] = True

# -----------------------
# ============================================================
# BLOQUE: SIDEBAR DE MERCADO (ZONA, MERCADO, TF, MODO)
# ============================================================
# SIDEBAR  CONFIGURACION                    #CARGAR DATOS
# -----------------------

st.sidebar.header("⚙️ Configuración de mercado")

zona = st.sidebar.selectbox(
    "🕒 Zona horaria",
    ["UTC", "Bogotá", "New York", "Londres", "Madrid"],
    key="zona"
)
st.sidebar.divider()

mercados = {
    "Oro (XAU/USD)": "GC=F",
    "NASDAQ 100": "^NDX",
    "S&P 500": "^GSPC",
    "Cripto": "CRYPTO"
}
mercados_visibles = ["Cripto", "Oro (XAU/USD)"]
mercado_ui_default = str(st.session_state.get("mercado_nombre_ui", "Cripto"))
if mercado_ui_default not in mercados_visibles:
    mercado_ui_default = "Cripto"

mercado_nombre = st.sidebar.selectbox(
    "Selecciona mercado",
    mercados_visibles,
    index=mercados_visibles.index(mercado_ui_default),
    key="mercado_nombre_ui",
)
mercado_abierto = mercado_abierto_ahora(mercado_nombre)
if not mercado_abierto:
    st.sidebar.caption("Estado: Mercado cerrado (fin de semana, horario NY).")
else:
    st.sidebar.caption("Estado: Mercado abierto.")

crypto_symbol = None
binance_symbol = None
td_symbol = None
if mercado_nombre == "Cripto":
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
else:
    ticker = mercados[mercado_nombre]
    td_symbol = TD_SYMBOLS.get(mercado_nombre)

timeframes = {
    "5 minutos": "5m",
    "15 minutos": "15m",
    "30 minutos": "30m",
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
auto_refresh = st.sidebar.toggle(
    "Auto refresh (gráficos)",
    value=st.session_state.get("auto_refresh_charts", True),
    key="auto_refresh_charts"
)
AUTO_REFRESH_SEC = 60
REFRESH_MS = AUTO_REFRESH_SEC * 1000
STAR_REFRESH_SEC = AUTO_REFRESH_SEC
live_mode_enabled = st.sidebar.toggle(
    "Live Mode con Bybit Futures",
    value=st.session_state.get("live_mode_enabled", False),
    disabled=(mercado_nombre != "Cripto")
)
st.session_state["live_mode_enabled"] = bool(live_mode_enabled)
if mercado_nombre != "Cripto":
    st.sidebar.caption("Live Mode solo disponible para Cripto.")
intervalo = timeframes[timeframe_nombre]

if st.sidebar.button("🧹 Limpiar memoria de la Estrella"):
    st.session_state.memoria_estrella = []
    st.sidebar.success("Memoria limpiada.")

if mercado_abierto:
    st.caption(explicacion_horario(zona))
else:
    st.caption("Mercado cerrado (fin de semana). Cuando abra, evalúa la sesión y la calidad del horario.")

if st.session_state.get("show_quick_guide", False):
    st.markdown("<div class='et-guide-backdrop'></div>", unsafe_allow_html=True)
    with st.container(key="onboarding_modal"):
        col_title, col_close = st.columns([4, 2])
        with col_title:
            st.markdown("### 📘 Guía rápida ")
        with col_close:
            if st.button("Entendido, cerrar guia", key="btn_close_quick_guide"):
                st.session_state["show_quick_guide"] = False
                rerun_app()
        st.markdown(cargar_guia_rapida_markdown())

# -----------------------
# TÍTULO
# -----------------------

# ============================================================
# BLOQUE: TITULO PRINCIPAL + LOGO
# ============================================================
def _img_to_base64(path: str) -> str:
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("ascii")
    except Exception:
        return ""

_title_star_path = os.path.join(os.path.dirname(__file__), "assets", "estrella Red.png")
_title_star_b64 = _img_to_base64(_title_star_path)
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
# ============================================================
# BLOQUE: CABECERA OPERATIVA (SESION + CALIDAD)
# ============================================================
col1, col2 = st.columns(2)

with col1:
    st.subheader("🕒 Sesión actual")
    sesion = sesion_actual(zona) if mercado_abierto else "Mercado cerrado"
    st.write(sesion)

with col2:
    st.subheader("📊 Calidad del horario")
    calidad = calidad_horario(zona) if mercado_abierto else "cerrado"
    calidad_txt = calidad
    st.write(calidad_txt)
    # Panel de notas y boton "Guardar notas" desactivados temporalmente.
    guardar_notas = False

# ========= DATOS DE MERCADO =========

mercado = mercado_nombre

# ============================================================
# BLOQUE: PARAMETROS DE DATOS (INTERVALOS, PERIODOS, MODO)
# ============================================================
# 2) Mapear timeframes UI -> yfinance (IMPORTANTE)
INTERVAL_MAP = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "60m",  # yfinance suele ir mejor con 60m que "1h"
}

# 3) Periodo recomendado según intervalo (para evitar vac?os)
PERIOD_MAP = {
    "1m": "1d",
    "5m": "5d",
    "15m": "5d",
    "30m": "1mo",
    "1h": "3mo",
    "60m": "3mo",
    "4h": "12mo",
    "1d": "3y",
}
periodo = PERIOD_MAP.get(intervalo, "5d")
modo_estructural = modo_lectura == "Estructural (1D + 4H)"
datos_1d = None
datos_4h = None

# ============================================================
# BLOQUE: PIPELINE DE CARGA DE DATOS (BINANCE / TWELVEDATA)
# ============================================================
try:
    TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
    live_mode_requested = bool(st.session_state.get("live_mode_enabled")) and mercado == "Cripto" and not modo_estructural
    use_binance_live = live_mode_requested
    live_fallback_reason = ""
    if modo_estructural and bool(st.session_state.get("live_mode_enabled")) and mercado == "Cripto":
        st.sidebar.caption("Live Mode se desactiva en lectura estructural 1D+4H.")
    if use_binance_live and not WEBSOCKETS_AVAILABLE:
        live_fallback_reason = "Paquete websockets no disponible en este servidor."
        logger.warning("Live Bybit Futures desactivado: %s", live_fallback_reason)
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

        needs_seed = store.symbol != binance_symbol or store.interval != binance_interval or store.get_df().empty
        if needs_seed:
            seed_df, seed_err = fetch_klines(binance_symbol, binance_interval, limit=500)
            if seed_df is None or seed_df.empty:
                live_fallback_reason = seed_err or f"Bybit Futures REST sin datos para {binance_symbol} {binance_interval}."
                logger.warning("Live Bybit Futures seed fallido: %s", live_fallback_reason)
                use_binance_live = False
            else:
                store.seed(seed_df, binance_symbol, binance_interval)

        if use_binance_live:
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
                retry_df, retry_err = fetch_klines(binance_symbol, binance_interval, limit=500)
                if retry_df is not None and not retry_df.empty:
                    store.seed(retry_df, binance_symbol, binance_interval)
                    datos = store.get_df()
                else:
                    ws_err = store.get_last_error() if hasattr(store, "get_last_error") else None
                    live_fallback_reason = ws_err or retry_err or "No se recibieron velas live desde Bybit Futures."
                    logger.warning("Live Bybit Futures sin datos, fallback activado: %s", live_fallback_reason)
                    use_binance_live = False
    if not use_binance_live:
        stop_prev = st.session_state.get("binance_stop")
        if stop_prev:
            stop_prev.set()
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
        if live_mode_requested:
            st.sidebar.warning("Live Bybit Futures no disponible en este servidor. Usando datos alternos (TwelveData/yfinance).")
            if live_fallback_reason:
                st.sidebar.caption(f"Detalle live: {live_fallback_reason[:220]}")
                st.session_state["binance_live_last_error"] = live_fallback_reason
    else:
        st.session_state["binance_live_last_error"] = ""

    live_ws_reconnects = 0
    store_dbg = st.session_state.get("binance_store")
    if store_dbg and hasattr(store_dbg, "get_ws_reconnects"):
        try:
            live_ws_reconnects = int(store_dbg.get_ws_reconnects())
        except Exception:
            live_ws_reconnects = 0
    st.session_state["binance_ws_reconnects"] = live_ws_reconnects
    st.session_state["binance_live_effective"] = bool(use_binance_live)

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

if (
    mercado == "Cripto"
    and not modo_estructural
    and bool(st.session_state.get("live_mode_enabled"))
):
    effective_live = bool(st.session_state.get("binance_live_effective", False))
    ws_reconnects = int(st.session_state.get("binance_ws_reconnects", 0))
    pill_class = "is-live" if effective_live else "is-fallback"
    status_label = "LIVE Bybit Futures" if effective_live else "Fallback datos"
    meta_label = (
        f"WS reintentos: {ws_reconnects}"
        if effective_live
        else f"TwelveData/yfinance | WS reintentos: {ws_reconnects}"
    )
    with st.container(key="live_status_badge"):
        st.markdown(
            f"""
            <div class="et-live-pill {pill_class}">
              <span class="dot"></span>
              <span>{status_label}</span>
              <span class="meta">| {meta_label}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

# -----------------------
# ANÁLISIS DE CONTEXTO
# -----------------------

# ============================================================
# BLOQUE: ANALISIS DE CONTEXTO + NOTAS
# ============================================================
tendencia, estado_rsi = contexto_mercado(datos)
estado_bb = interpretar_bollinger(datos)
sesion = sesion_actual(zona) if mercado_abierto else "Mercado cerrado"
col_bb = st.container()
with col_bb:
    st.dataframe(
        datos[["BBL", "BBM", "BBU"]].tail(3),
        use_container_width=False,
        width=420,
        height=140
    )
# with col_notas:
#     notas_bollinger = st.text_area(
#         "",
#         key="notas_bollinger",
#         height=70,
#         label_visibility="collapsed",
#         placeholder="Notas"
#     )
#     if guardar_notas:
#         base_dir = os.path.join(os.path.expanduser("~"), "Documents", "Archivos")
#         os.makedirs(base_dir, exist_ok=True)
#         nombre_archivo = f"notas_bollinger_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
#         ruta_salida = os.path.join(base_dir, nombre_archivo)
#         with open(ruta_salida, "w", encoding="utf-8") as f:
#             f.write(notas_bollinger or "")
#         st.success(f"Notas guardadas en: {ruta_salida}")
estado_bb = interpretar_bollinger(datos)

# ============================================================
# BLOQUE: MEMORIA - GUARDADO DE RECUERDOS
# ============================================================
def guardar_recuerdo(contexto):
    st.session_state.memoria_estrella.append(contexto)

    # l?mite de memoria (protección)
    if len(st.session_state.memoria_estrella) > 20:
        st.session_state.memoria_estrella.pop(0)


# -----------------------
# ESTADO DE LA ESTRELLA (N?CLEO)
# -----------------------
# ============================================================
# BLOQUE: NUCLEO DE DECISION DE LA ESTRELLA
# ============================================================
if "star_last_ts" not in st.session_state:
    st.session_state["star_last_ts"] = 0.0
if "star_force_refresh" not in st.session_state:
    st.session_state["star_force_refresh"] = True
if _HAS_AUTOREFRESH and auto_refresh and not hasattr(st, "fragment"):
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
    estado = normalizar_objeto_ui(estado)
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
    evento_estrella = normalizar_objeto_ui(evento_estrella)

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
    estado = normalizar_objeto_ui(payload["estado"])
    influencia = normalizar_objeto_ui(payload["influencia"])
    razones = normalizar_objeto_ui(payload["razones"])
    resumen = normalizar_texto_ui(payload["resumen"])
    decision_txt = normalizar_texto_ui(payload["decision_txt"])
    frase_pedagogica = normalizar_texto_ui(payload["frase_pedagogica"])
    evento_estrella = normalizar_objeto_ui(payload["evento_estrella"])

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

estado = normalizar_objeto_ui(estado)
resumen = normalizar_texto_ui(resumen)
decision_txt = normalizar_texto_ui(decision_txt)
frase_pedagogica = normalizar_texto_ui(frase_pedagogica)

if es_premium and st.session_state.get("debug_v13", False):
    debug = resumen_estado_humano(estado, usuario)
    dorado_estado = estado.get("dorado_v13") or {}
    d = debug.get("direccion", {})
    dor = debug.get("dorado", {})
    r = debug.get("rojo", {})
    e = debug.get("ensenar", {})
    estructura = estado.get("estructura_1d_4h")

    def _diag_row(label: str, value: str, value_class: str = "") -> str:
        cls = f"et-premium-value {value_class}".strip()
        return (
            f"<div class='et-premium-row'>"
            f"<span class='et-premium-label'>{html.escape(label)}:</span>"
            f"<span class='{cls}'>{html.escape(value)}</span>"
            f"</div>"
        )

    rows = []
    rows.append(
        _diag_row(
            "Dorado hits",
            f"{st.session_state.get('dorado_hits', 0)} | miss: {st.session_state.get('dorado_miss', 0)}",
        )
    )
    rows.append(
        _diag_row(
            "Direccion dominante",
            f"{d.get('valor', 'N/A')} ({d.get('fortaleza', 'N/A')})",
        )
    )
    rows.append(
        _diag_row(
            "Scores",
            f"alcista {d.get('score_alcista', 'N/A')} | "
            f"bajista {d.get('score_bajista', 'N/A')} | "
            f"umbral {d.get('umbral', 'N/A')}",
        )
    )

    if isinstance(estructura, dict):
        rows.append(
            _diag_row(
                "Estructura",
                f"1D {estructura.get('direccion_1d', 'NEUTRAL')} | "
                f"4H {estructura.get('direccion_4h', 'NEUTRAL')} | "
                f"{estructura.get('alineacion', 'N/A')}",
            )
        )

    dorado_activo = bool(dor.get("activo", False))
    rows.append(
        _diag_row(
            "Dorado",
            "ACTIVO" if dorado_activo else "NO ACTIVO",
            "is-active" if dorado_activo else "is-inactive",
        )
    )

    if dorado_activo:
        micro = dor.get("micro_score")
        umbral = dor.get("umbral")
        if micro is not None and umbral is not None:
            rows.append(_diag_row("Micro-score", f"{micro} | Umbral {umbral}"))

        rr_estimado = dorado_estado.get("rr_estimado")
        if rr_estimado is not None:
            rows.append(_diag_row("RR estimado", str(rr_estimado)))

        accion_dorado = dorado_estado.get("accion", estado.get("accion", ""))
        resumen_dorado = dorado_estado.get("resumen", estado.get("mensaje", ""))
        if accion_dorado:
            rows.append(_diag_row("Accion", str(accion_dorado)))
        if resumen_dorado:
            rows.append(_diag_row("Resumen", str(resumen_dorado)))

    nivel_riesgo = str(r.get("nivel") or "sin evaluacion activa")
    risk_class = "is-risk" if nivel_riesgo.lower() in ("alto", "muy alto") else ""
    rows.append(_diag_row("Rojo (riesgo)", nivel_riesgo, risk_class))

    if bool(e.get("premium", False)):
        rows.append(_diag_row("Ensenar", "Disponible para tu cuenta"))

    razones_dorado = dorado_estado.get("razones") or dor.get("razones") or []
    razones_riesgo = r.get("razones") or []

    dorado_list_html = ""
    if dorado_activo and razones_dorado:
        items = "".join(f"<li>{html.escape(str(x))}</li>" for x in razones_dorado)
        dorado_list_html = (
            "<div class='et-premium-subtitle'>Razones Dorado</div>"
            f"<ul class='et-premium-list'>{items}</ul>"
        )

    riesgo_list_html = ""
    if razones_riesgo:
        items = "".join(f"<li>{html.escape(str(x))}</li>" for x in razones_riesgo)
        riesgo_list_html = (
            "<div class='et-premium-subtitle'>Razones de Riesgo</div>"
            f"<ul class='et-premium-list'>{items}</ul>"
        )

    diag_html = (
        "<div class='et-premium-diag'>"
        "<div class='et-title'>Diagnostico Premium</div>"
        f"<div class='et-premium-grid'>{''.join(rows)}{dorado_list_html}{riesgo_list_html}</div>"
        "</div>"
    )
    st.markdown(diag_html, unsafe_allow_html=True)

    scanner_health = _leer_scanner_health()
    if scanner_health:
        counters = scanner_health.get("counters", {})
        if not isinstance(counters, dict):
            counters = {}
        lat = scanner_health.get("latency_ms", {})
        if not isinstance(lat, dict):
            lat = {}
        cycle_lat = (lat.get("cycle", {}) if isinstance(lat.get("cycle", {}), dict) else {})
        notif_lat = (lat.get("notification", {}) if isinstance(lat.get("notification", {}), dict) else {})
        cycles_total = int(counters.get("cycles_total", 0) or 0)
        cycles_failed = int(counters.get("cycles_failed", 0) or 0)
        failed_pct = (cycles_failed / cycles_total * 100.0) if cycles_total > 0 else 0.0
        alerts_sent_total = int(counters.get("alerts_sent", 0) or 0)
        alerts_failed_total = int(counters.get("alerts_failed", 0) or 0)
        status = str(scanner_health.get("status", "unknown")).strip().upper()
        hb = str(scanner_health.get("last_heartbeat_utc", "")).strip()
        rss_mb = _safe_float_num(scanner_health.get("rss_mb"), 0.0)

        st.markdown("### Salud operativa")
        ch1, ch2, ch3, ch4, ch5, ch6 = st.columns(6)
        ch1.metric("Estado", status)
        ch2.metric("Ciclos", cycles_total)
        ch3.metric("Fallo ciclos", f"{failed_pct:.1f}%")
        ch4.metric("Lat ciclo prom.", f"{_safe_float_num(cycle_lat.get('avg_ms'), 0.0):.0f} ms")
        ch5.metric("Lat envio prom.", f"{_safe_float_num(notif_lat.get('avg_ms'), 0.0):.0f} ms")
        ch6.metric("RSS worker", f"{rss_mb:.0f} MB")
        if hb:
            st.caption(f"Heartbeat: {hb} UTC | Alertas enviadas: {alerts_sent_total} | fallidas: {alerts_failed_total}")

        notif = scanner_health.get("notifications", {})
        if isinstance(notif, dict):
            notif_rows = []
            for channel in ("telegram", "email", "windows"):
                item = notif.get(channel, {})
                if not isinstance(item, dict):
                    item = {}
                item_lat = item.get("latency_ms", {})
                if not isinstance(item_lat, dict):
                    item_lat = {}
                attempts = int(item.get("attempts", 0) or 0)
                sent = int(item.get("sent", 0) or 0)
                failed = int(item.get("failed", 0) or 0)
                sent_pct = (sent / attempts * 100.0) if attempts > 0 else 0.0
                notif_rows.append(
                    {
                        "Canal": channel,
                        "Intentos": attempts,
                        "Enviadas": sent,
                        "Fallidas": failed,
                        "% exito": round(sent_pct, 2),
                        "Lat prom. ms": round(_safe_float_num(item_lat.get("avg_ms"), 0.0), 2),
                    }
                )
            st.dataframe(pd.DataFrame(notif_rows), use_container_width=True, hide_index=True)

# ============================================================
# BLOQUE: PANEL PRINCIPAL FLOTANTE (DECISION + CTA)
# ============================================================
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

if debug_line:
    debug_color = COL_TEXT_2 if not mercado_abierto else color_por_esfera(estado.get("esfera", ""))
    debug_line = debug_line.replace(
        'style="',
        f'style="color:{debug_color}; ',
        1,
    )

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

if st.session_state.pop("play_star_rain", False):
    _ox, _oy = _star_burst_origin(st.session_state.get("panel_pos", "Arriba derecha"))
    render_star_rain_overlay(_ox, _oy)
# ========= [PANEL PRINCIPAL FLOTANTE - FIN] =========

dorado_estado = estado.get("dorado_v13", estado.get("dorado"))
rojo_estado = estado.get("rojo_v13", estado.get("rojo"))
esfera_actual_txt = str(estado.get("esfera", "")).lower()

dorado_activo = False
if isinstance(dorado_estado, dict):
    dorado_activo = bool(dorado_estado) and bool(dorado_estado.get("activo", True))
else:
    dorado_activo = bool(dorado_estado)
if not dorado_activo and ("dorad" in esfera_actual_txt or "🟡" in esfera_actual_txt):
    dorado_activo = True

if not mercado_abierto:
    st.subheader("⏸️ Mercado cerrado")
    st.write("La lectura operativa queda en pausa hasta la apertura del mercado.")
elif dorado_activo:
    st.subheader("🟡 Ventaja detectada (Dorado)")
    if isinstance(dorado_estado, dict):
        resumen_dorado = dorado_estado.get("resumen", estado.get("mensaje", "Ventaja detectada."))
        accion_dorado = dorado_estado.get("accion", estado.get("accion", "OPERAR CON DISCIPLINA"))
        rr_estimado = dorado_estado.get("rr_estimado")
        razones_dorado = dorado_estado.get("razones") or []
    else:
        resumen_dorado = estado.get("mensaje", "Ventaja detectada.")
        accion_dorado = estado.get("accion", "OPERAR CON DISCIPLINA")
        rr_estimado = None
        razones_dorado = []

    st.write(resumen_dorado)
    st.write("Acción:", accion_dorado)
    if rr_estimado is not None:
        st.write("RR estimado:", rr_estimado)
    if razones_dorado:
        st.write("Razones:")
        for r in razones_dorado:
            st.write("•", r)
else:
    st.subheader("🔵 Azul")
    st.write(estado.get("mensaje", "No hay ventaja suficiente ahora."))

if isinstance(rojo_estado, dict) and rojo_estado:
    st.subheader("🔴 Riesgo (Rojo)")
    st.write(f"Nivel: **{rojo_estado.get('nivel', estado.get('riesgo', 'N/A'))}**")
    razones_rojo = rojo_estado.get("razones") or []
    if razones_rojo:
        st.write("Razones:")
        for r in razones_rojo:
            st.write("•", r)

if recalc_estrella and mercado_abierto:
    guardar_recuerdo(evento_estrella)

# ============================================================
# BLOQUE: MENSAJE PRINCIPAL DE LA ESTRELLA
# ============================================================
# ========= [MENSAJE ESTRELLA - INICIO] =========
esfera = estado.get("esfera", "🔵 Azul (análisis)")
mensaje = estado.get("mensaje", "")
mensaje = "\n".join([line for line in mensaje.splitlines() if line.strip()])
modo_mensaje = "Estructural (1D+4H)" if modo_estructural else "Tendencial"

accent = color_por_esfera(esfera)
msg_placeholder = st.empty()
last_msg = st.session_state.get("last_star_msg_html")
msg_html = f"""
<div class="et-star-msg" style="--accent:{accent};">
  <div class="et-title">📣 Mensaje de la Estrella</div>
  <div class="et-mini">Modo: {html.escape(modo_mensaje)}</div>
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
# ============================================================
# BLOQUE: RAZONES / TRAZABILIDAD / RECUERDOS RELEVANTES
# ============================================================
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
# ============================================================
# BLOQUE: ENSENAR + EVENTOS PREMIUM + ADVERTENCIAS
# ============================================================
ens = estado.get("ensenar")
if es_premium and (ens or ("🔴" in esfera_actual or "🔵" in esfera_actual)):
    st.markdown("<div class='et-title'>🧠 Enseñar</div>", unsafe_allow_html=True)

    if "🔴" in esfera_actual or "🔵" in esfera_actual:
        razon_usuario = st.text_input("¿Por qué NO operar aquí?", key="razon_usuario")
        if st.button("🧠 Enseñar Estrella") and razon_usuario.strip():
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
            st.markdown(f"- {ev.get('fecha', '')} • {ev.get('titulo', '')} ({ev.get('sesion', '')})")

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


# ============================================================
# BLOQUE: RENDER VISUAL DE ESTRELLA (FONDO) + ESTADO
# ============================================================
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

st.session_state["idle_star_color"] = color_estado
render_estrella_fondo(color_estado, mercado_abierto)

# Oculto el gráfico frontal de la estrella para dejar solo el render de fondo.
SHOW_FOREGROUND_STAR = False
if SHOW_FOREGROUND_STAR:
    st.plotly_chart(
        esfera_visual(color_estado, all_lit=not mercado_abierto),
        use_container_width=False,
        key="esfera_visual"
    )
if esfera_visual_txt == "🔴":
    st.error("🔴 ESTRELLA EN MODO RIESGO • Riesgo sobre la ventaja")
elif esfera_visual_txt == "🟡":
    if dir_v13 == "ALCISTA":
        st.warning("🟡 ESTRELLA EN MODO TENDENCIA • Alcista")
    elif dir_v13 == "BAJISTA":
        st.warning("🟡 ESTRELLA EN MODO TENDENCIA • Bajista")
    else:
        st.warning("🟡 ESTRELLA EN MODO TENDENCIA • Ventaja detectada")
else:
    st.info("🔵 ESTRELLA EN MODO NEUTRAL • Observa")

# -----------------------
# GRÁFICO
# -----------------------
# ============================================================
# BLOQUE: RENDER DE GRAFICOS DE MERCADO
# ============================================================
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
# ============================================================
# BLOQUE: WRAPPERS DE RENDER (FRAGMENT/AUTOREFRESH)
# ============================================================
def _render_rsi_chart(datos):
    return


def _render_fragment(fn, *args):
    if hasattr(st, "fragment") and auto_refresh:
        st.fragment(run_every=REFRESH_MS / 1000)(fn)(*args)
    else:
        if auto_refresh and (not hasattr(st, "fragment")) and (not _HAS_AUTOREFRESH):
            st.sidebar.warning("Auto refresh requiere st.fragment o streamlit-autorefresh.")
        fn(*args)


def _render_star_section():
    pass


# ============================================================
# BLOQUE: EJECUCION FINAL DE RENDERS
# ============================================================
_render_fragment(_render_star_section)
main_chart_placeholder = st.empty()
_render_fragment(_render_main_chart, datos, use_binance_live, ticker, main_chart_placeholder, mercado_abierto)
_render_fragment(_render_rsi_chart, datos)











