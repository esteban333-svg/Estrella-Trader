from typing import Optional, Dict, Any, List
from datetime import datetime
import yfinance as yf
import pandas as pd

try:
    import pandas_ta as ta
    _HAS_PANDAS_TA = True
except Exception:
    ta = None
    _HAS_PANDAS_TA = False


def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def _rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def _bbands(series: pd.Series, length: int = 20, std: float = 2.0) -> pd.DataFrame:
    mid = series.rolling(window=length).mean()
    dev = series.rolling(window=length).std(ddof=0)
    upper = mid + (std * dev)
    lower = mid - (std * dev)
    return pd.DataFrame({"BBL": lower, "BBM": mid, "BBU": upper})


def _to_float_or_nan(value: Any) -> float:
    try:
        out = float(value)
    except Exception:
        return float("nan")
    return out


def obtener_datos_robusto(ticker: str, period: str = "5d", interval: str = "15m") -> pd.DataFrame:
    """
    Descarga robusta: evita crasheos cuando yfinance devuelve vacío o falla.
    Retorna DataFrame vacío si no hay datos.
    """
    try:
        data = yf.download(ticker, period=period, interval=interval, progress=False)
        if isinstance(data, pd.DataFrame) and not data.empty:
            return data

        # Fallback: history() a veces funciona cuando download() no
        tk = yf.Ticker(ticker)
        data2 = tk.history(period=period, interval=interval)
        if isinstance(data2, pd.DataFrame) and not data2.empty:
            return data2

        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def obtener_precio_live(ticker: str) -> Optional[float]:
    """
    Toma el último close de velas 1m como proxy de precio live.
    (No es tick real, pero se mueve y sirve para demo visual).
    """
    try:
        df = yf.download(ticker, period="1d", interval="1m", progress=False)
        if df is None or df.empty:
            return None
        return float(df["Close"].iloc[-1])
    except Exception:
        return None
# ========= [ENSEÑAR v1.2-C-A - INICIO] =========

def _risk_value(r: str) -> int:
    r = (r or "").lower().strip()
    return {"bajo": 1, "medio": 2, "alto": 3}.get(r, 2)


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def detectar_eventos_mercado(estado: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Detecta SOLO los 4 eventos enseñables definidos.
    No depende de indicadores extra: usa flags si existen.
    Si no existen, no rompe (solo no detecta).
    """
    eventos: List[Dict[str, Any]] = []

    # Flags recomendadas (si no existen, quedan False)
    ruptura_real = bool(estado.get("ruptura_real", False))
    fallo_ruptura = bool(estado.get("fallo_ruptura", False))
    cambio_tendencia = bool(estado.get("cambio_tendencia", False))
    expansion_volatilidad = bool(estado.get("expansion_volatilidad", False))

    # Contexto m?nimo
    mercado = estado.get("mercado", estado.get("symbol", ""))
    sesion = estado.get("sesion", estado.get("sesion", ""))
    esfera = estado.get("esfera", "")
    riesgo = estado.get("riesgo", "Medio")
    decision = estado.get("decision", "")

    def mk(tipo: str, titulo: str, texto: str) -> Dict[str, Any]:
        return {
            "tipo": tipo,
            "titulo": titulo,
            "texto": texto,
            "fecha": _now_str(),
            "mercado": mercado,
            "sesion": sesion,
            "esfera": esfera,
            "riesgo": riesgo,
            "decision": decision,
            # importante: esto es Ã¢â‚¬Å“evento mercadoÃ¢â‚¬Â, no Ã¢â‚¬Å“evento usuarioÃ¢â‚¬Â
            "origen": "mercado",
        }

    if ruptura_real:
        eventos.append(mk(
            "ruptura",
            "Ruptura real",
            "El mercado salio de una zona clara y cambio de ritmo. "
            "En estos puntos la prisa suele costar; la confirmacion protege."
        ))

    if fallo_ruptura:
        eventos.append(mk(
            "fallo_ruptura",
            "Fallo de ruptura",
            "No toda ruptura continúa. Cuando el precio no sostiene, "
            "la confirmación vale más que la velocidad."
        ))

    if cambio_tendencia:
        eventos.append(mk(
            "cambio_tendencia",
            "Cambio de tendencia",
            "El mercado cambio su logica (regimen). "
            "Lo que funcionaba antes puede dejar de funcionar aqui."
        ))

    if expansion_volatilidad:
        eventos.append(mk(
            "expansion_volatilidad",
            "Expansion tras compresion",
            "La calma suele preceder al movimiento. "
            "Despues de una compresionn, es normal que aparezca emocion: observa antes de actuar."
        ))

    return eventos


def construir_ensenar(
        usuario: Dict[str, Any],
        estado: Dict[str, Any],
        recuerdos_relevantes: Optional[List[Dict[str, Any]]] = None
) -> Optional[Dict[str, Any]]:
    """
    Premium-only. Devuelve UNA sola enseñanza (breve, orientadora, sin pasos).
    No aparece si no aporta algo real.
    Formato final:
      { titulo, texto, cierre_opcional, fuente, prioridad }
    """
    if not usuario.get("es_premium", False):
        return None

    decision = (estado.get("decision") or "").upper().strip()
    riesgo = (estado.get("riesgo") or "Medio").capitalize()

    # Señal Ã¢â‚¬Å“estructura válida pero intención no claraÃ¢â‚¬Â (si no tienes flags, usa texto del estado)
    # Si no existe, no pasa nada.
    estructura_valida = bool(estado.get("estructura_valida", False))
    intencion_clara = bool(estado.get("intencion_clara", True))  # True por defecto

    # Eventos de mercado detectados (si existen flags)
    eventos_mercado = estado.get("eventos_mercado", []) or []

    # Prioridad: 1) Protección riesgo alto, 2) estructura sin intención, 3) evento mercado relevante
    # Mantén una sola intervención.
    if decision == "NO OPERAR" and _risk_value(riesgo) == 3:
        return {
            "titulo": "Protección activa",
            "texto": "Evitar operar en riesgo alto protege tu capital y tu calma. "
                     "Aquí la disciplina pesa más que la oportunidad.",
            "cierre": "La Estrella protege primero.",
            "fuente": "decision",
            "prioridad": 1
        }

    if decision == "OBSERVAR" and estructura_valida and not intencion_clara:
        return {
            "titulo": "Estructura sin intención",
            "texto": "La estructura es válida, pero el mercado aún no muestra intención clara. "
                     "Operar aquí suele venir de anticipación, no de confirmación.",
            "cierre": "Observar también es operar bien.",
            "fuente": "contexto",
            "prioridad": 2
        }

    # Si hay un evento de mercado, enseña una idea (sin empujar a operar)
    if eventos_mercado:
        ev = eventos_mercado[0]  # toma el primero; puedes ordenar si quieres
        return {
            "titulo": ev.get("titulo", "Contexto de mercado"),
            "texto": ev.get("texto", ""),
            "cierre": "El mercado cambia. El criterio observa.",
            "fuente": "mercado",
            "prioridad": 3
        }

    # Si no hay nada claro, silencio.
    return None


# ========= [ENSEÑAR v1.2-C-A - FIN] =========
# ========= [ENSEÑAR - INICIO] ========

from typing import Optional, Dict, Any, List, Callable


def _bool_from_memoria(memoria: Any, clave: str, default: bool = False) -> bool:
    if memoria is None:
        return default
    if isinstance(memoria, dict):
        return bool(memoria.get(clave, default))
    return bool(getattr(memoria, clave, default))


def _get(memoria: Any, clave: str, default=None):
    if memoria is None:
        return default
    if isinstance(memoria, dict):
        return memoria.get(clave, default)
    return getattr(memoria, clave, default)


def _norm(txt: str) -> str:
    return (txt or "").strip()


def _risk_value(r: str) -> int:
    r = (r or "").lower().strip()
    return {"bajo": 1, "medio": 2, "alto": 3}.get(r, 2)


def _decision_norm(d: str) -> str:
    d = (d or "").upper().strip()
    # Normaliza posibles variantes
    if d in ("NO_OPERAR", "NO-OPERAR"):
        return "NO OPERAR"
    return d


def calcular_contenido_ensenable(
        usuario: Dict[str, Any],
        estado: Dict[str, Any],
        memoria: Optional[Any] = None
) -> Optional[Dict[str, Any]]:
    """
    v1.2-C-A (Premium fuerte): guía tipo navegador.
    Devuelve None si:
      - usuario no es premium
      - no hay insight claro
    Devuelve dict con:
      - ahora: acción inmediata (1 línea)
      - porque: motivo corto (1–2 líneas)
      - proximo: siguiente condición/confirmación (1 línea)
      - tono/tipo/prioridad
    """
    if not usuario.get("es_premium", False):
        return None

    decision = _decision_norm(estado.get("decision", ""))
    riesgo = _norm(estado.get("riesgo", "Medio")).capitalize()
    esfera = _norm(estado.get("esfera", ""))
    razones: List[str] = estado.get("razones", []) or []

    # Señales m?nimas (si existen en tu estado; si no, quedan False y no rompen)
    sesion = _norm(estado.get("sesion", estado.get("sesión", "")))
    estructura_confirmada = bool(estado.get("estructura_confirmada", False))
    rsi = estado.get("rsi", None)  # puede ser float o None
    cerca_ema200 = bool(estado.get("cerca_ema200", False))  # opcional
    cerca_bollinger = bool(estado.get("cerca_bollinger", False))  # opcional

    # Memoria (si existe)
    impulsividad = _bool_from_memoria(memoria, "indica_impulsividad", False)
    sobreoperar_sesion_baja = _bool_from_memoria(memoria, "sobreoperar_sesion_baja", False)
    entrar_sin_confirmacion = _bool_from_memoria(memoria, "entrar_sin_confirmacion", False)

    # --- Motor de reglas (cada regla produce una "gu?a" corta) ---
    def guia(
            titulo: str,
            ahora: str,
            porque: str,
            proximo: str,
            tipo: str,
            prioridad: int,
            etiqueta: str,
    ) -> Dict[str, Any]:
        return {
            "titulo": titulo,
            "ahora": ahora,
            "porque": porque,
            "proximo": proximo,
            "tipo": tipo,
            "prioridad": prioridad,
            "etiqueta": etiqueta,  # id interno de la regla
        }

    candidatos: List[Dict[str, Any]] = []

    # REGLA CORE Ã¢â‚¬â€ Estructura válida pero intención no clara (navegador puro)
    estructura_valida = any(
        "estructura" in (r or "").lower()
        and ("válida" in (r or "").lower() or "valida" in (r or "").lower())
        for r in razones
    )
    intencion_no_clara = (
            "intención" in (estado.get("frase_pedagogica", "") or "").lower()
            and "no clara" in (estado.get("frase_pedagogica", "") or "").lower()
    )

    if decision == "OBSERVAR" and estructura_valida and intencion_no_clara:
        candidatos.append(guia(
            titulo="🧠 Guía de la Estrella",
            ahora="Ahora: Mantente en OBSERVAR.",
            porque="La estructura es valida, pero todavia no hay intencion. Entrar aqui es adivinar.",
            proximo="Proximo paso: espera confirmación (cierre claro + ruptura/rechazo) antes de considerar operar.",
            tipo="lectura",
            prioridad=2,
            etiqueta="estructura_ok_intencion_no_clara",
        ))

    # REGLA 1 Ã¢â‚¬â€ Fuera de sesión + observar: gu?a protectora (muy Ã¢â‚¬Å“navegadorÃ¢â‚¬Â)
    if sesion.lower().startswith("fuera") and decision in ("OBSERVAR", "NO OPERAR"):
        candidatos.append(guia(
            titulo="🧠 Guía de la Estrella",
            ahora="Ahora: Mantente en OBSERVAR.",
            porque="Fuera de sesión la calidad baja y el ruido sube. No hay prisa.",
            proximo="Próximo paso: vuelve en sesión principal y busca confirmación limpia.",
            tipo="proteccion",
            prioridad=1,
            etiqueta="fuera_sesion_proteccion",
        ))

    # REGLA 2 Ã¢â‚¬â€ OBSERVAR + riesgo medio/alto + impulsividad: prevención personalizada
    if decision == "OBSERVAR" and _risk_value(riesgo) >= 2 and impulsividad:
        candidatos.append(guia(
            titulo="🧠 Guía de la Estrella",
            ahora="Ahora: Observa sin intervenir.",
            porque="Este es el contexto donde tu mente suele apurarse. Hoy el control es ventaja.",
            proximo="Próximo paso: espera una vela de confirmación antes de considerar operar.",
            tipo="disciplina",
            prioridad=1,
            etiqueta="observar_impulsividad",
        ))

    # REGLA 3 Ã¢â‚¬â€ NO OPERAR + riesgo alto: refuerzo simple (sin medidor)
    if decision == "NO OPERAR" and _risk_value(riesgo) == 3:
        candidatos.append(guia(
            titulo="🧠 Guía de la Estrella",
            ahora="Ahora: NO OPERAR.",
            porque="El riesgo alto no se negocia. Proteger capital también es progreso.",
            proximo="Próximo paso: espera que el riesgo baje o que el contexto se ordene.",
            tipo="proteccion",
            prioridad=1,
            etiqueta="no_operar_riesgo_alto",
        ))

    # REGLA 4 Ã¢â‚¬â€ OBSERVAR + RSI neutral + entrar sin confirmación (memoria): Ã¢â‚¬Å“no te adelantesÃ¢â‚¬Â con razón personal
    if decision == "OBSERVAR" and rsi is not None and 40 <= float(rsi) <= 60 and entrar_sin_confirmacion:
        candidatos.append(guia(
            titulo="🧠 Guía de la Estrella",
            ahora="Ahora: OBSERVAR en neutral.",
            porque="En RSI neutral tu error típico es anticipar. Aquí se pierde por impaciencia.",
            proximo="Próximo paso: espera salida de neutral + estructura clara.",
            tipo="correccion",
            prioridad=2,
            etiqueta="rsi_neutral_memoria",
        ))

    # REGLA 5 Ã¢â‚¬â€ OBSERVAR + cerca EMA200: gu?a de lectura (técnico, sobrio)
    if decision == "OBSERVAR" and cerca_ema200:
        candidatos.append(guia(
            titulo="🧠 Guía de la Estrella",
            ahora="Ahora: Lee reacción en EMA 200.",
            porque="EMA 200 suele actuar como zona de decisión. No se adivina: se observa.",
            proximo="Próximo paso: confirma rechazo/ruptura con velas antes de actuar.",
            tipo="lectura",
            prioridad=3,
            etiqueta="ema200_lectura",
        ))

    # REGLA 6 Ã¢â‚¬â€ OBSERVAR + cerca Bollinger: evitar entradas por Ã¢â‚¬Å“toqueÃ¢â‚¬Â
    if decision == "OBSERVAR" and cerca_bollinger:
        candidatos.append(guia(
            titulo="🧠 Guía de la Estrella",
            ahora="Ahora: No operes por tocar banda.",
            porque="Bollinger no es señal por sí sola. El contexto manda, no el borde.",
            proximo="Próximo paso: espera compresión/expansión + confirmación.",
            tipo="lectura",
            prioridad=3,
            etiqueta="bollinger_lectura",
        ))

    # REGLA 7 Ã¢â‚¬â€ OPERAR + estructura confirmada + riesgo bajo: gu?a de ejecución responsable
    if decision == "OPERAR" and estructura_confirmada and _risk_value(riesgo) == 1:
        candidatos.append(guia(
            titulo="🧠 Guía de la Estrella",
            ahora="Ahora: Ejecuta con disciplina.",
            porque="La ventaja no está en entrar, está en gestionar el riesgo y respetar el plan.",
            proximo="Próximo paso: define invalidación y tamaño antes de la entrada.",
            tipo="ejecucion",
            prioridad=2,
            etiqueta="operar_disciplina",
        ))

    # REGLA 8 Ã¢â‚¬â€ OBSERVAR + riesgo medio/alto + sobreoperar_sesion_baja: aviso fino (sin sermón)
    if decision == "OBSERVAR" and _risk_value(riesgo) >= 2 and sobreoperar_sesion_baja:
        candidatos.append(guia(
            titulo="🧠 Guía de la Estrella",
            ahora="Ahora: Mantén la calma.",
            porque="En horarios de baja calidad tu historial muestra sobreoperación.",
            proximo="Próximo paso: limita intentos o espera mejor sesión.",
            tipo="proteccion",
            prioridad=2,
            etiqueta="sesion_baja_sobreoperar",
        ))

    if not candidatos:
        return None

    # Elegir el más importante (prioridad menor = más importante)
    candidatos.sort(key=lambda x: x["prioridad"])
    elegido = candidatos[0]

    # Salida final (compacta, tipo navegador)
    direccion_actual = estado.get("direccion_v13", "NEUTRAL")

    if direccion_actual == "ALCISTA":
        accion = "Posible entrada alcista"
        resumen = "Ventaja alcista detectada (micro-score Dorado activo)"
    elif direccion_actual == "BAJISTA":
        accion = "Posible entrada bajista"
        resumen = "Ventaja bajista detectada (micro-score Dorado activo)"
    else:
        accion = "Sin ventaja clara"
        resumen = "No hay dirección dominante suficiente."
    return {
        "titulo": elegido["titulo"],
        "ahora": elegido["ahora"],
        "porque": elegido["porque"],
        "proximo": elegido["proximo"],
        "tipo": elegido["tipo"],
        "prioridad": elegido["prioridad"],
        "etiqueta": elegido["etiqueta"],
        # útil para debug, pero si no quieres ruido, no lo muestres
        "esfera": esfera,
        "riesgo": riesgo,
        "decision": decision,
        "razones": razones[:4],
    }


def aplicar_ensenar(
        estado: Dict[str, Any],
        decision: Optional[str],
        usuario: Dict[str, Any],
        memoria: Optional[Any] = None
) -> Dict[str, Any]:
    """
    Inyecta estado["ensenar"] sin sobrescribir estado["decision"].
    """
    estado = dict(estado)
    # Premium "Enseñar" (gu?a tipo navegador)
    estado["ensenar"] = calcular_contenido_ensenable(
        usuario=usuario,
        estado=estado,
        memoria=memoria  # si no tienes memoria aqu?, pasa None
    )
    if "rsi" in estado:
        try:
            estado["rsi_actual"] = round(float(estado["rsi"]), 2)
        except (TypeError, ValueError):
            pass

    return estado


# ========= [ENSEÑAR - FIN] =========


def obtener_datos(ticker, periodo="5d", intervalo="15m"):
    """
    Descarga datos del mercado seleccionado
    """
    data = obtener_datos_robusto(ticker, period=periodo, interval=intervalo)

    if data.empty:
        raise ValueError("No se pudieron obtener datos del mercado")

    # Aplanar columnas si vienen como MultiIndex
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    return data
import yfinance as yf
from typing import Optional
import pandas as pd

def obtener_precio_live(ticker: str) -> Optional[float]:
    """
    Proxy de precio en 'vivo' usando velas 1m.
    (No es tick real, pero se mueve para demo).
    """
    try:
        df = yf.download(ticker, period="1d", interval="1m", progress=False)
        if df is None or df.empty:
            return None
        return float(df["Close"].iloc[-1])
    except Exception:
        return None

def calcular_indicadores(data):
    data = data.copy()
    for col in ("Open", "High", "Low", "Close", "Volume"):
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

    # EMAs
    if _HAS_PANDAS_TA:
        data["EMA_20"] = ta.ema(data["Close"], length=20)
        data["EMA_50"] = ta.ema(data["Close"], length=50)
        data["EMA_200"] = ta.ema(data["Close"], length=200)
    else:
        data["EMA_20"] = _ema(data["Close"], length=20)
        data["EMA_50"] = _ema(data["Close"], length=50)
        data["EMA_200"] = _ema(data["Close"], length=200)

    # RSI
    if _HAS_PANDAS_TA:
        data["RSI"] = ta.rsi(data["Close"], length=14)
    else:
        data["RSI"] = _rsi(data["Close"], length=14)

    # Bandas de Bollinger
    if _HAS_PANDAS_TA:
        bb = ta.bbands(data["Close"], length=20, std=2)
    else:
        bb = _bbands(data["Close"], length=20, std=2)

    # Asignación segura (independiente de versión)
    if bb is None or getattr(bb, "empty", False):
        data["BBL"] = pd.NA
        data["BBM"] = pd.NA
        data["BBU"] = pd.NA
    else:
        data["BBL"] = bb.iloc[:, 0]
        data["BBM"] = bb.iloc[:, 1]
        data["BBU"] = bb.iloc[:, 2]

    return data


def contexto_mercado(data):
    """
    Analiza el contexto general del mercado
    """
    ultimo = data.iloc[-1]

    precio = ultimo["Close"]
    ema_200 = ultimo["EMA_200"]
    rsi = ultimo["RSI"]

    # Tendencia (manejo seguro de NaN/None)
    if pd.isna(precio) or pd.isna(ema_200):
        tendencia = "neutral"
    elif precio > ema_200:
        tendencia = "alcista"
    else:
        tendencia = "bajista"

    # Estado RSI
    if pd.isna(rsi):
        estado = "neutral"
    elif rsi > 70:
        estado = "sobrecompra"
    elif rsi < 30:
        estado = "sobreventa"  # bien
    else:
        estado = "neutral"

    return tendencia, estado


def interpretar_bollinger(data):
    ultimo = data.iloc[-1]

    close = ultimo["Close"]
    bbl = ultimo["BBL"]
    bbu = ultimo["BBU"]

    if pd.isna(close) or pd.isna(bbl) or pd.isna(bbu):
        return "normal"

    ancho = (bbu - bbl) / close

    if pd.isna(ancho):
        return "normal"
    if ancho < 0.01:
        return "compresión"
    elif close > bbu:
        return "ruptura_alcista"
    elif close < bbl:
        return "ruptura_bajista"
    else:
        return "normal"


def interpretar_rsi(rsi):
    if rsi > 70:
        return "El RSI está en sobrecompra. Entrar aquí suele ser riesgoso."
    elif rsi < 30:
        return "El RSI está en sobreventa. Puede haber rebote, pero confirma."
    else:
        return "El RSI está en zona neutral. Espera estructura o confirmación."


def advertencia_trade(tendencia, estado_rsi):
    if estado_rsi == "sobrecompra" and tendencia == "alcista":
        return "⚠️ Precio extendido. Espera retroceso."
    if estado_rsi == "sobreventa" and tendencia == "bajista":
        return "⚠️ Presión bajista fuerte. No anticipes."
    return "✅ Contexto sano. Observa estructura antes de entrar."


def estructura_mercado(data):
    data = data.copy()

    # Protección por si las columnas vienen raras
    columnas = list(data.columns)

    if "High" not in columnas or "Low" not in columnas or "Close" not in columnas:
        return "Estructura no disponible ⚠️"

    max_20 = data["High"].rolling(window=20).max().iloc[-1]
    min_20 = data["Low"].rolling(window=20).min().iloc[-1]
    precio = data["Close"].iloc[-1]

    if precio > max_20:
        return "Ruptura alcista 📈"
    elif precio < min_20:
        return "Ruptura bajista 📉"
    else:
        return "Rango / consolidación ⏸️"


def evaluar_estado_estrella(datos, sesion, calidad):
    """
    Evalúa el estado interno de la Estrella Trader
    y decide qué esfera lidera según el contexto real del mercado
    """

    ultimo = datos.iloc[-1]

    precio = ultimo["Close"]
    ema_200 = ultimo["EMA_200"]
    rsi = ultimo["RSI"]

    if pd.isna(precio) or pd.isna(ema_200) or ema_200 == 0:
        distancia_ema = None
    else:
        distancia_ema = abs(precio - ema_200) / ema_200

    # -----------------------
    # CONDICIONES ROJAS
    # -----------------------
    condiciones_rojas = []

    # RSI extremo fuerte
    if not pd.isna(rsi) and (rsi > 75 or rsi < 25):
        condiciones_rojas.append("RSI en zona extrema fuerte")
        # super bien
    # Precio muy extendido
    if distancia_ema is not None and distancia_ema > 0.012:
        condiciones_rojas.append("Precio muy alejado de EMA 200")

    # Sesión lenta
    if sesion == "Tokio":
        condiciones_rojas.append("Sesión Tokio (baja intención)")
    # Fuera de sesión principal
    if sesion == "Fuera de sesión":
        condiciones_rojas.append("Fuera de sesión principal")

    # Horario malo
    if calidad == "baja":
        condiciones_rojas.append("Horario de baja calidad")

    # 🔴 ROJO FUERTE
    if len(condiciones_rojas) >= 2:
        return {
            "esfera": "🔴 Roja (protección total)",
            "riesgo": "Muy alto",
            "accion": "No operar",
            "razones": condiciones_rojas,  # ✅ AÑADIR
            "mensaje": (
                    "La Estrella activa protección total.\n\n"
                    "El mercado no está en un estado saludable:\n- "
                    + "\n- ".join(condiciones_rojas) +
                    "\n\nPreservar capital también es una decisión profesional."
            )
        }

    # 🔴 ROJO SUAVE
    if len(condiciones_rojas) == 1:
        return {
            "esfera": "🔴 Roja (precaución)",
            "riesgo": "Alto",
            "accion": "Esperar o reducir tamaño",
            "razones": condiciones_rojas,  # ✅ AÑADIR
            "mensaje": (
                    "Hay una señal de advertencia en el mercado:\n- "
                    + condiciones_rojas[0] +
                    "\n\nNo fuerces decisiones. Observa con calma."
            )
        }

    # -----------------------
    # CONDICIONES AZULES
    # -----------------------
    razones_azules = []
    if (not pd.isna(rsi)) and (distancia_ema is not None) and (42 <= rsi <= 58) and (distancia_ema <= 0.01):
        razones_azules = [
            "RSI en zona neutral",
            "Precio cerca de EMA 200",
            "Estructura técnica válida"
        ]
        return {
            "esfera": "🔵 Azul (análisis)",
            "riesgo": "Medio",
            "accion": "Observar estructura",
            "razones": razones_azules,  # ✅ AÑADIR
            "mensaje": (
                "El mercado muestra orden técnico, pero aún no hay intención clara.\n"
                "Lee las velas, no te adelantes.\n"
                "La paciencia también es una decisión."
            )
        }

    if pd.isna(precio) or pd.isna(ema_200):
        sesgo = "neutral"
    else:
        sesgo = "alcista" if precio > ema_200 else "bajista"
    razones_doradas = [
        "Sesión favorable",
        "Tendencia definida",
        "Riesgo bajo"
    ]

    return {
        "esfera": "🟡 Dorada (criterio y decisión)",
        "riesgo": "Bajo",
        "accion": "Posible entrada consciente",
        "razones": razones_doradas,
        "mensaje": (
            "El contexto acompaña.\n"
            f"Sesión favorable con sesgo {sesgo}.\n\n"
            "Decide con paciencia y gestión de riesgo."
        )
    }


import numpy as np


def calcular_score_azul(data: pd.DataFrame) -> dict:
    """
    v1.3 — Núcleo Azul
    Devuelve:
    {
        score_alcista,
        score_bajista,
        umbral,
        direccion,
        volatilidad_nivel
    }
    """

    data = data.copy()

    # =========================
    # 1Ã¯Â¸ÂÃ¢Æ’Â£ ESTRUCTURA (Peso mayor)
    # =========================
    score_alcista = 0
    score_bajista = 0

    highs = data["High"].rolling(5).max()
    lows = data["Low"].rolling(5).min()

    # Últimos valores
    if highs.iloc[-1] > highs.iloc[-2] and lows.iloc[-1] > lows.iloc[-2]:
        score_alcista += 3

    if highs.iloc[-1] < highs.iloc[-2] and lows.iloc[-1] < lows.iloc[-2]:
        score_bajista += 3

    # =========================
    # 2Ã¯Â¸ÂÃ¢Æ’Â£ EMAs
    # =========================
    ema20 = data["EMA_20"].iloc[-1]
    ema50 = data["EMA_50"].iloc[-1]
    ema200 = data["EMA_200"].iloc[-1]

    # Evitar comparaciones cuando hay NaN/None por falta de datos
    if not (pd.isna(ema20) or pd.isna(ema50) or pd.isna(ema200)):
        if ema20 > ema50 > ema200:
            score_alcista += 2
        elif ema20 < ema50 < ema200:
            score_bajista += 2

    # =========================
    # 3Ã¯Â¸ÂÃ¢Æ’Â£ RSI
    # =========================
    rsi = data["RSI"].iloc[-1]

    if not pd.isna(rsi):
        if rsi > 55:
            score_alcista += 1
        elif rsi < 45:
            score_bajista += 1

    # =========================
    # 4Ã¯Â¸ÂÃ¢Æ’Â£ VOLATILIDAD (ATR)
    # =========================
    data["TR"] = np.maximum(
        data["High"] - data["Low"],
        np.maximum(
            abs(data["High"] - data["Close"].shift()),
            abs(data["Low"] - data["Close"].shift())
        )
    )

    data["ATR"] = data["TR"].rolling(14).mean()

    atr_actual = data["ATR"].iloc[-1]
    atr_media = data["ATR"].rolling(50).mean().iloc[-1]

    vol_ratio = atr_actual / atr_media if atr_media != 0 else 1

    if vol_ratio > 1.3:
        umbral = 4
        volatilidad_nivel = "Alta"
    elif vol_ratio < 0.7:
        umbral = 2
        volatilidad_nivel = "Baja"
    else:
        umbral = 3
        volatilidad_nivel = "Normal"

    # =========================
    # 5Ã¯Â¸ÂÃ¢Æ’Â£ DIRECCIÃƒâ€œN DOMINANTE
    # =========================
    diferencia = score_alcista - score_bajista

    if diferencia >= umbral:
        direccion = "ALCISTA"
    elif -diferencia >= umbral:
        direccion = "BAJISTA"
    else:
        direccion = "NEUTRAL"

    return {
        "score_alcista": score_alcista,
        "score_bajista": score_bajista,
        "umbral": umbral,
        "direccion": direccion,
        "volatilidad_nivel": volatilidad_nivel
    }


def voz_estrella(estado, tono="mentor"):
    esfera = estado["esfera"]

    if tono == "protector":
        return "Prefiero que hoy no arriesgues. El mercado no está claro."

    if tono == "tecnico":
        return "Las estructuras se alinean. Ejecuta solo si confirmas."

    if "AZUL" in esfera or "Azul" in esfera:
        return (
            "🌟 Estrella:\n"
            "Observa con atención.\n\n"
            f"{estado['mensaje']}\n\n"
            "El análisis paciente es una ventaja que pocos usan."
        )

    if "DORADO" in esfera or "Dorada" in esfera:
        return (
            "🌟 Estrella:\n"
            "Este es un buen contexto.\n\n"
            f"{estado['mensaje']}\n\n"
            "Recuerda: una buena entrada empieza con una buena decisión."
        )

    # Fallback
    return "🌟 Estrella:\nEstoy evaluando el mercado."


def voz_estrella_con_memoria(estado, recuerdos):
    """
    Modula la voz de la Estrella según la influencia de la memoria
    """
    from memoria import influencia_de_memoria

    influencia = influencia_de_memoria(estado)
    nivel = influencia["nivel"]

    mensaje_base = voz_estrella(estado)

    if nivel == 0:
        return mensaje_base

    if nivel == 1:
        return (
                mensaje_base +
                "\n\n🧠 La Estrella recuerda experiencias previas similares. "
                "Observa con atención."
        )  # muy bien

    if nivel == 2:
        return (
                mensaje_base +
                "\n\n⚠️ Advertencia de memoria: este contexto ha generado errores antes. "
                "Reduce riesgo y confirma más de lo normal."
        )

    if nivel == 3:
        return (
            "🧠 Memoria de protección activa.\n\n"
            "La Estrella ha visto pérdidas repetidas en este contexto.\n"
            "No es recomendable operar ahora."
        )

    return mensaje_base


def estado_estrella(datos, sesion, calidad):
    """
    v1.3 — Estado basado en Núcleo Azul
    """

    resultado_azul = calcular_score_azul(datos)
    direccion = resultado_azul["direccion"]

    if direccion == "ALCISTA":
        return {
            "esfera": "🔵 Azul (análisis)",
            "riesgo": "Evaluando...",
            "accion": "Esperando ventaja",
            "mensaje": "Dirección alcista dominante."
        }

    elif direccion == "BAJISTA":
        return {
            "esfera": "🔵 Azul (análisis)",
            "riesgo": "Evaluando...",
            "accion": "Esperando ventaja",
            "mensaje": "Dirección bajista dominante."
        }

    else:
        return {
            "esfera": "🔵 Azul (análisis)",
            "riesgo": "Neutral",
            "accion": "Observar",
            "mensaje": (
                "Neutral real: el mercado no ofrece ventaja clara.\n"
                "Espera confirmación estructural."
            )
        }



import numpy as np
from typing import Dict, Any, Optional


def _atr14(df: pd.DataFrame) -> float:
    """ATR(14) simple para umbrales adaptativos."""
    d = df.copy()
    required = {"High", "Low", "Close"}
    if not required.issubset(set(d.columns)):
        return 1e-9
    for col in ("High", "Low", "Close"):
        d[col] = pd.to_numeric(d[col], errors="coerce")
    tr = np.maximum(
        d["High"] - d["Low"],
        np.maximum(
            (d["High"] - d["Close"].shift()).abs(),
            (d["Low"] - d["Close"].shift()).abs()
        )
    )
    atr = pd.Series(tr).rolling(14).mean()
    if atr.empty or pd.isna(atr.iloc[-1]):
        return 1e-9
    val = _to_float_or_nan(atr.iloc[-1])
    if pd.isna(val):
        return 1e-9
    return max(val, 1e-9)


def _nivel_resistencia(df: pd.DataFrame, n: int = 50) -> float:
    """Resistencia simple: máximo reciente."""
    if "High" not in df.columns:
        return float("nan")
    high = pd.to_numeric(df["High"], errors="coerce")
    return _to_float_or_nan(high.tail(n).max())


def _nivel_soporte(df: pd.DataFrame, n: int = 50) -> float:
    """Soporte simple: mínimo reciente."""
    if "Low" not in df.columns:
        return float("nan")
    low = pd.to_numeric(df["Low"], errors="coerce")
    return _to_float_or_nan(low.tail(n).min())


def calcular_micro_score_dorado(df: pd.DataFrame, direccion: str) -> Optional[Dict[str, Any]]:
    """
    v1.3 — Dorado (micro-score de ventaja)
    - Solo evalúa SI ya hay dirección dominante (ALCISTA/BAJISTA).
    - Devuelve None si no hay ventaja suficiente.
    """

    direccion = (direccion or "").upper().strip()
    if direccion not in ("ALCISTA", "BAJISTA"):
        return None

    d = df.copy()
    for col in ("High", "Low", "Close", "EMA_20", "EMA_50", "RSI"):
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce")
    close = _to_float_or_nan(d["Close"].iloc[-1] if "Close" in d.columns and len(d) else float("nan"))
    ema20 = _to_float_or_nan(d["EMA_20"].iloc[-1] if "EMA_20" in d.columns and len(d) else float("nan"))
    ema50 = _to_float_or_nan(d["EMA_50"].iloc[-1] if "EMA_50" in d.columns and len(d) else float("nan"))
    if pd.isna(close) or pd.isna(ema20) or pd.isna(ema50):
        return None
    atr = _atr14(d)
    if pd.isna(atr) or atr <= 0:
        return None

    # =========================
    # Micro-score Dorado (0–7)
    # =========================
    score = 0
    razones = []

    # 1) Retroceso saludable hacia EMA20/EMA50 (distancia en ATR)
    dist_ema20 = abs(close - ema20) / atr
    dist_ema50 = abs(close - ema50) / atr

    # Queremos "cerca", no extendido: <= ~1.2 ATR
    if min(dist_ema20, dist_ema50) <= 0.8:
        score += 2
        razones.append("Retroceso saludable cerca de EMA20/EMA50 (no extendido).")
    elif min(dist_ema20, dist_ema50) <= 1.2:
        score += 1
        razones.append("Precio razonablemente cerca de EMAs (retroceso aceptable).")

    # 2) Zona técnica simple (proximidad a soporte/resistencia reciente)
    soporte = _nivel_soporte(d, n=50)
    resistencia = _nivel_resistencia(d, n=50)
    if pd.isna(soporte) or pd.isna(resistencia):
        return None

    # distancia relativa en ATR
    dist_soporte = abs(close - soporte) / atr
    dist_resistencia = abs(resistencia - close) / atr

    if direccion == "ALCISTA":
        # ventaja si está más cerca de soporte que de resistencia
        if dist_soporte <= 1.2 and dist_resistencia >= 1.2:
            score += 2
            razones.append("Precio en zona favorable (más cerca de soporte que de resistencia).")
        elif dist_soporte <= 1.2:
            score += 1
            razones.append("Precio cerca de soporte reciente (zona interesante).")
    else:  # BAJISTA
        if dist_resistencia <= 1.2 and dist_soporte >= 1.2:
            score += 2
            razones.append("Precio en zona favorable (más cerca de resistencia que de soporte).")
        elif dist_resistencia <= 1.2:
            score += 1
            razones.append("Precio cerca de resistencia reciente (zona interesante).")

    # 3) RR m?nimo estimado (simple con ATR)
    # Idea: si dirección alcista, objetivo "hacia resistencia"; si bajista, "hacia soporte".
    # Stop aproximado: 1 ATR.
    stop_atr = 1.0
    if direccion == "ALCISTA":
        target_atr = max((resistencia - close) / atr, 0.0)
    else:
        target_atr = max((close - soporte) / atr, 0.0)

    rr = (target_atr / stop_atr) if stop_atr > 0 else 0.0

    if rr >= 2.0:
        score += 2
        razones.append("RR estimado >= 1:2 (ventaja de ejecucion).")
    elif rr >= 1.5:
        score += 1
        razones.append("RR estimado aceptable (~1:1.5).")

    # Umbral Dorado (activacion)
    # =========================
    # Umbral Dorado adaptativo (no regalar)
    # =========================
    # Si el mercado esta muy volatil, exigimos mas (evita falsas "ventajas")
    # Si esta normal, umbral estandar
    # Si esta calmado, permitimos ventaja parcial pero aun estricta
    tr = (d["High"] - d["Low"]).abs()
    atr50 = float(tr.rolling(50).mean().iloc[-1]) if len(d) >= 50 else float(tr.mean())
    atr50 = max(atr50, 1e-9)
    vol_ratio = atr / atr50

    if vol_ratio > 1.3:
        UMBRAL_DORADO = 5
    elif vol_ratio < 0.8:
        UMBRAL_DORADO = 3
    else:
        UMBRAL_DORADO = 4

    # Regla extra: si RR es alto (>=2), permitimos activar con 1 punto menos.
    ajuste_rr = 1 if rr >= 2.0 else 0
    umbral_final = max(2, UMBRAL_DORADO - ajuste_rr)

    if score < umbral_final:
        return None

    # Texto final sobrio
    if direccion == "ALCISTA":
        if dist_soporte <= 1.5:
            score += 2
            razones.append("Precio en zona interesante (cerca de soporte reciente).")
        elif dist_soporte <= 2.2:
            score += 1
            razones.append("Precio relativamente cercano a soporte (zona posible).")


    else:  # BAJISTA

        if dist_resistencia <= 1.5:

            score += 2

            razones.append("Precio en zona interesante (cerca de resistencia reciente).")

        elif dist_resistencia <= 2.2:

            score += 1

            razones.append("Precio relativamente cercano a resistencia (zona posible).")

    # Textos consistentes (NO dependen de variables externas)
    if direccion == "ALCISTA":
        accion = "Posible preparación alcista"
        resumen = f"Ventaja alcista detectada (micro-score {score}/{umbral_final})"
    else:
        accion = "Posible preparación bajista"
        resumen = f"Ventaja bajista detectada (micro-score {score}/{umbral_final})"

    return {
        "activo": True,
        "direccion": direccion,
        "micro_score": score,
        "umbral": UMBRAL_DORADO,
        "accion": accion,
        "resumen": resumen,
        "razones": razones[:5],  # limpio
        "rr_estimado": round(rr, 2),
    }


from typing import Dict, Any, Optional


def _riesgo_verbal(score: int) -> str:
    if score <= 2:
        return "Bajo"
    if score <= 5:
        return "Moderado"
    if score <= 8:
        return "Alto"
    return "Muy alto"


def calcular_micro_score_rojo(
        df: pd.DataFrame,
        direccion: str,
        dorado: Optional[Dict[str, Any]] = None,
        impacto_memoria: int = 0
) -> Dict[str, Any]:
    """
    v1.3 — Rojo (micro-score de riesgo acumulativo)
    - No bloquea
    - Devuelve nivel verbal
    - Idealmente se usa cuando Dorado está activo (hay posible ejecución)
    """

    direccion = (direccion or "").upper().strip()
    d = df.copy()
    for col in ("High", "Low", "Close", "EMA_20", "RSI"):
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce")

    close = _to_float_or_nan(d["Close"].iloc[-1] if "Close" in d.columns and len(d) else float("nan"))
    ema20 = _to_float_or_nan(d["EMA_20"].iloc[-1] if "EMA_20" in d.columns and len(d) else float("nan"))
    if pd.isna(close) or pd.isna(ema20):
        return {
            "micro_score": 0,
            "nivel": "Bajo",
            "razones": ["Datos incompletos para evaluar riesgo (Close/EMA20)."],
        }

    # ATR para umbrales adaptativos
    atr = _atr14(d)
    if pd.isna(atr) or atr <= 0:
        atr = 1e-9

    # 1) Volatilidad extrema (ATR ratio)
    # Reutilizamos ATR(14) y un proxy con ATR(50) calculado rápido
    tr = (d["High"] - d["Low"]).abs()
    atr50 = float(tr.rolling(50).mean().iloc[-1]) if len(d) >= 50 else float(tr.mean())
    atr50 = max(atr50, 1e-9)
    vol_ratio = atr / atr50

    score = 0
    razones = []

    if vol_ratio > 1.8:
        score += 3
        razones.append("Volatilidad extrema (ATR muy elevado).")
    elif vol_ratio > 1.3:
        score += 2
        razones.append("Volatilidad alta (ATR elevado).")

    # 2) Extensión excesiva respecto EMA20 (en ATR)
    ext = abs(close - ema20) / atr
    if ext > 2.2:
        score += 3
        razones.append("Precio muy extendido respecto a EMA20.")
    elif ext > 1.8:
        score += 2
        razones.append("Precio extendido respecto a EMA20.")

    # 3) Cercan?a a zona estructural Ã¢â‚¬Å“duraÃ¢â‚¬Â
    soporte = _nivel_soporte(d, n=50)
    resistencia = _nivel_resistencia(d, n=50)

    dist_soporte = abs(close - soporte) / atr
    dist_resistencia = abs(resistencia - close) / atr

    # En alcista: riesgo si estás pegado a resistencia
    if direccion == "ALCISTA" and dist_resistencia <= 0.8:
        score += 2
        razones.append("Cercanía a resistencia fuerte (posible rechazo).")

    # En bajista: riesgo si estás pegado a soporte
    if direccion == "BAJISTA" and dist_soporte <= 0.8:
        score += 2
        razones.append("Cercanía a soporte fuerte (posible rebote).")

    # 4) Memoria (por ahora manual / 0 mientras no tengamos SQLite)
    # impacto_memoria puede ser: 0, -1, -2 (negativa), +1 (positiva)
    if impacto_memoria <= -2:
        score += 3
        razones.append("Memoria histórica muy negativa en contexto similar.")
    elif impacto_memoria == -1:
        score += 2
        razones.append("Memoria negativa en contexto similar.")

    # 5) Divergencia RSI (simple proxy)
    # Si RSI está cayendo mientras precio sube (o viceversa) en últimas velas
    if "RSI" in d.columns and len(d) >= 6:
        rsi_last = d["RSI"].tail(6).values
        close_last = d["Close"].tail(6).values
        # Pendiente simple
        rsi_slope = float(rsi_last[-1] - rsi_last[0])
        price_slope = float(close_last[-1] - close_last[0])

        if direccion == "ALCISTA" and price_slope > 0 and rsi_slope < 0:
            score += 2
            razones.append("Divergencia RSI (precio sube, RSI cae).")
        if direccion == "BAJISTA" and price_slope < 0 and rsi_slope > 0:
            score += 2
            razones.append("Divergencia RSI (precio cae, RSI sube).")

    nivel = _riesgo_verbal(score)

    return {
        "micro_score": score,
        "nivel": nivel,
        "razones": razones[:6]
    }


def construir_estado_final(
        datos: pd.DataFrame,
        impacto_memoria: int = 0
) -> Dict[str, Any]:
    """
    v1.3 - Nucleo unico.
    Toda la salida principal (esfera, decision, mensaje, debug) se define aqui.
    """
    resultado_azul = calcular_score_azul(datos)
    direccion = (resultado_azul.get("direccion") or "NEUTRAL").upper().strip()

    dorado = calcular_micro_score_dorado(datos, direccion)
    rojo = None
    if dorado is not None:
        rojo = calcular_micro_score_rojo(
            datos,
            direccion,
            dorado=dorado,
            impacto_memoria=impacto_memoria
        )

    vol = resultado_azul.get("volatilidad_nivel", "Normal")
    score_alcista = int(resultado_azul.get("score_alcista", 0))
    score_bajista = int(resultado_azul.get("score_bajista", 0))
    umbral = int(resultado_azul.get("umbral", 0))

    estado = {
        "version": "v1.3",
        "direccion_v13": direccion,
        "volatilidad_v13": vol,
        "score_alcista_v13": score_alcista,
        "score_bajista_v13": score_bajista,
        "umbral": umbral,
        "dorado_v13": dorado,
        "rojo_v13": rojo,
        "esfera": "🔵 Azul (análisis)",
        "decision": "OBSERVAR",
        "accion": "OBSERVAR",
        "riesgo": "Bajo",
        "mensaje": "No hay ventaja suficiente ahora. Mantente en OBSERVAR.",
        "frase_pedagogica": "No hay ventaja suficiente ahora. Mantente en OBSERVAR.",
        "mensaje_direccion": (
            "Dirección alcista dominante." if direccion == "ALCISTA"
            else "Dirección bajista dominante." if direccion == "BAJISTA"
            else "Dirección neutral: sin ventaja estructural."
        ),
    }

    if dorado is not None:
        estado.update({
            "esfera": "🟡 Dorada (criterio y decisión)",
            "decision": "OPERAR CON DISCIPLINA",
            "accion": dorado.get("accion", "Posible ventaja"),
            "riesgo": (rojo or {}).get("nivel", "Moderado"),
            "mensaje": dorado.get("resumen", "Ventaja detectada. Ejecuta con disciplina."),
            "frase_pedagogica": dorado.get("resumen", "Ventaja detectada. Ejecuta con disciplina."),
        })

    if rojo is not None and rojo.get("nivel") in ("Alto", "Muy alto"):
        estado.update({
            "esfera": "🔴 Roja (riesgo)",
            "decision": "NO OPERAR",
            "accion": "NO OPERAR",
            "riesgo": rojo.get("nivel", "Alto"),
            "mensaje": "Riesgo alto detectado. No operar hasta nueva lectura.",
            "frase_pedagogica": "Riesgo alto detectado. No operar hasta nueva lectura.",
        })

    estado["debug_v13"] = {
        "azul": resultado_azul,
        "dorado_activo": dorado is not None,
        "micro_score_dorado": (dorado or {}).get("micro_score"),
        "rojo": rojo,
    }
    return estado


def construir_estado_final_estructural(
        datos_1d: pd.DataFrame,
        datos_4h: pd.DataFrame,
        impacto_memoria: int = 0
) -> Dict[str, Any]:
    """
    v1.3 - Modo estructural.
    1D define direccion macro y 4H valida ejecucion.
    Mantiene el mismo contrato de salida para la UI.
    """
    azul_1d = calcular_score_azul(datos_1d)
    azul_4h = calcular_score_azul(datos_4h)

    direccion_1d = (azul_1d.get("direccion") or "NEUTRAL").upper().strip()
    direccion_4h = (azul_4h.get("direccion") or "NEUTRAL").upper().strip()

    if direccion_1d == "NEUTRAL":
        alineacion = "SIN_SESGO_MACRO"
    elif direccion_4h == direccion_1d:
        alineacion = "ALINEADO"
    elif direccion_4h == "NEUTRAL":
        alineacion = "RETROCESO_O_PAUSA"
    else:
        alineacion = "CONFLICTO"

    vol_4h = azul_4h.get("volatilidad_nivel", "Normal")
    score_alcista_1d = int(azul_1d.get("score_alcista", 0))
    score_bajista_1d = int(azul_1d.get("score_bajista", 0))
    umbral_1d = int(azul_1d.get("umbral", 0))

    estado = {
        "version": "v1.3",
        "modo_lectura": "estructural_1d_4h",
        "direccion_v13": direccion_1d,
        "volatilidad_v13": vol_4h,
        "score_alcista_v13": score_alcista_1d,
        "score_bajista_v13": score_bajista_1d,
        "umbral": umbral_1d,
        "dorado_v13": None,
        "rojo_v13": None,
        "esfera": "🔵 Azul (análisis)",
        "decision": "OBSERVAR",
        "accion": "OBSERVAR",
        "riesgo": "Bajo",
        "mensaje": "No hay ventaja suficiente ahora. Mantente en OBSERVAR.",
        "frase_pedagogica": "No hay ventaja suficiente ahora. Mantente en OBSERVAR.",
        "mensaje_direccion": (
            "Direccion alcista dominante (macro 1D)." if direccion_1d == "ALCISTA"
            else "Direccion bajista dominante (macro 1D)." if direccion_1d == "BAJISTA"
            else "Direccion neutral en 1D: sin sesgo estructural."
        ),
        "estructura_1d_4h": {
            "direccion_1d": direccion_1d,
            "direccion_4h": direccion_4h,
            "alineacion": alineacion,
        },
    }

    # Conflicto fuerte de marcos: bloqueo de ejecucion.
    if alineacion == "CONFLICTO":
        rojo_conflicto = {
            "micro_score": 9,
            "nivel": "Alto",
            "razones": [
                f"Conflicto de marcos: 1D {direccion_1d} vs 4H {direccion_4h}.",
                "Espera alineacion antes de ejecutar.",
            ],
        }
        estado.update({
            "rojo_v13": rojo_conflicto,
            "esfera": "🔴 Roja (riesgo)",
            "decision": "NO OPERAR",
            "accion": "NO OPERAR",
            "riesgo": "Alto",
            "mensaje": "Conflicto 1D vs 4H. No operar hasta nueva alineacion.",
            "frase_pedagogica": "Conflicto 1D vs 4H. No operar hasta nueva alineacion.",
        })
    elif direccion_1d in ("ALCISTA", "BAJISTA"):
        dorado = calcular_micro_score_dorado(datos_4h, direccion_1d)
        estado["dorado_v13"] = dorado

        if dorado is not None:
            rojo = calcular_micro_score_rojo(
                datos_4h,
                direccion_1d,
                dorado=dorado,
                impacto_memoria=impacto_memoria
            )
            estado["rojo_v13"] = rojo
            estado.update({
                "esfera": "🟡 Dorada (criterio y decisión)",
                "decision": "OPERAR CON DISCIPLINA",
                "accion": dorado.get("accion", "Posible ventaja"),
                "riesgo": (rojo or {}).get("nivel", "Moderado"),
                "mensaje": dorado.get("resumen", "Ventaja detectada. Ejecuta con disciplina."),
                "frase_pedagogica": dorado.get("resumen", "Ventaja detectada. Ejecuta con disciplina."),
            })

            if rojo is not None and rojo.get("nivel") in ("Alto", "Muy alto"):
                estado.update({
                    "esfera": "🔴 Roja (riesgo)",
                    "decision": "NO OPERAR",
                    "accion": "NO OPERAR",
                    "riesgo": rojo.get("nivel", "Alto"),
                    "mensaje": "Riesgo alto detectado. No operar hasta nueva lectura.",
                    "frase_pedagogica": "Riesgo alto detectado. No operar hasta nueva lectura.",
                })
        elif alineacion == "RETROCESO_O_PAUSA":
            estado["mensaje"] = (
                "Macro 1D definida, pero 4H aun esta en pausa/retroceso. "
                "Mantente en OBSERVAR hasta ventaja clara."
            )
            estado["frase_pedagogica"] = estado["mensaje"]

    estado["debug_v13"] = {
        "modo_lectura": "estructural_1d_4h",
        "azul_1d": azul_1d,
        "azul_4h": azul_4h,
        "alineacion": alineacion,
        "dorado_activo": estado.get("dorado") is not None,
        "micro_score_dorado": (estado.get("dorado") or {}).get("micro_score"),
        "rojo": estado.get("rojo"),
    }
    return estado


def memoria_rapida(estado):
    esfera = estado.get("esfera", "")
    esfera_min = esfera.lower()
    if "roja" in esfera_min or esfera_min == "rojo":
        return "Este contexto ya ha causado pérdidas antes."
    if "azul" in esfera_min or esfera_min == "azul":
        return "Contexto repetido de operaciones técnicas."
    return "Buen momento histórico para observar patrones."


# âš ï¸ NOTA:
# Esta función pertenece a la capa UI.
# Se mantiene aqu? temporalmente para estabilidad.
def render_estado_estrella(estado):
    """
    Traduce el estado interno de la estrella a UI (emoji + mensaje)
    """

    if estado["esfera"] == "ROJA":
        return {
            "emoji": "🔴",
            "mensaje": "Modo protección — la estrella recomienda no operar"
        }

    if estado["esfera"] == "AZUL":
        return {
            "emoji": "🔵",
            "mensaje": "Modo operativo — condiciones técnicas favorables"
        }

    return {
        "emoji": "🟡",
        "mensaje": "Modo análisis — observa y aprende del mercado"
    }
    # --- LÓGICA FUTURA (ACTUALMENTE INALCANZABLE) ---
    # Este bloque queda documentado para evolución futura


def advertencia_por_memoria(*args):
    """
    Acepta llamadas antiguas y nuevas:
    - advertencia_por_memoria(estado)
    - advertencia_por_memoria(sesion, esfera)
    """
    from memoria import recuerdos_relevantes

    # Caso 1: se pasa estado completo
    if len(args) == 1 and isinstance(args[0], dict):
        esfera = args[0].get("esfera")

    # Caso 2: se pasa (sesion, esfera)
    elif len(args) == 2:
        esfera = args[1]

    else:
        return None  # falla silenciosa, no rompe la app

    recuerdos = recuerdos_relevantes(esfera)

    errores = [r for r in recuerdos if r.get("tipo") == "error"]

    if errores:
        return (
            f"La Estrella recuerda {len(errores)} experiencia(s) negativa(s) "
            "en un contexto similar.\nAvanza con extrema cautela."
        )

    return None


# -----------------------
# MEMORIA Y RECUERDOS
# -----------------------

def recuerdos_para_estado(estado):
    """
    Devuelve los recuerdos relevantes según la esfera dominante
    """
    from memoria import recuerdos_relevantes  # Importa aqu? para evitar ciclos
    return recuerdos_relevantes(estado["esfera"])


def influencia_de_memoria(estado):
    from memoria import recuerdos_relevantes

    recuerdos = recuerdos_relevantes(estado["esfera"])

    errores = [r for r in recuerdos if r.get("tipo") == "error"]
    aprendizajes = [r for r in recuerdos if r.get("tipo") == "aprendizaje"]
    observaciones = [r for r in recuerdos if r.get("tipo") == "observacion"]

    return {
        "errores": errores,
        "aprendizajes": aprendizajes,
        "observaciones": observaciones
    }


def decision_estrella(estado, influencia):
    """
    Decide qué hacer según estado actual y memoria
    """

    if estado["esfera"].startswith("🔴"):
        return {
            "decision": "NO OPERAR",
            "motivo": "Riesgo elevado detectado por la Estrella."
        }

    if influencia["errores"]:
        return {
            "decision": "NO OPERAR",
            "motivo": "La memoria recuerda errores en contextos similares."
        }

    if estado["esfera"].startswith("🟡") and estado["riesgo"].lower() == "bajo":
        return {
            "decision": "OPERAR CON DISCIPLINA",
            "motivo": "Contexto favorable con riesgo controlado."
        }

    return {
        "decision": "OBSERVAR",
        "motivo": "No hay confirmación suficiente."
    }

# ----------------------------------
# ESTRELLA TRADER v1.1 Ã¢â‚¬â€ ARQUITECTURA CONGELADA
# A partir de aqu?:
# - No se duplican funciones
# - No se agregan estados nuevos
# - Todo lo nuevo va a v1.2
# ----------------------------------
def construir_estado_v13(
    datos: pd.DataFrame,
    sesion: str,
    calidad: str,
    usuario: Dict[str, Any],
    memoria: Optional[Any] = None
) -> Dict[str, Any]:
    """
    V1.3 — Estado Final Unificado:
    Azul define dirección → Dorado decide si hay ventaja → Rojo mide riesgo.
    Devuelve un solo dict listo para UI.
    """

    # 1) Azul: dirección dominante
    azul = calcular_score_azul(datos)
    direccion = azul["direccion"]

    estado = {
        "version": "v1.3",
        "direccion": direccion,          # ALCISTA / BAJISTA / NEUTRAL
        "azul": azul,                    # debug opcional
        "dorado": None,
        "rojo": None,
        "esfera": "🔵 Azul (análisis)",
        "riesgo_verbal": "—",
        "accion": "OBSERVAR",
    }

    # 2) Dorado: SOLO si Azul NO es neutral
    if direccion in ("ALCISTA", "BAJISTA"):
        dorado = calcular_micro_score_dorado(datos, direccion)
        estado["dorado"] = dorado

        if dorado is not None:
            # Dorado activo -> cambia esfera a 🟡
            estado["esfera"] = "🟡 Dorada (ventaja)"
            estado["accion"] = "PREPARAR"

            # 3) Rojo: solo si Dorado está activo (para medir riesgo de ejecución)
            rojo = calcular_micro_score_rojo(datos, direccion, dorado=dorado, impacto_memoria=0)
            estado["rojo"] = rojo
            estado["riesgo_verbal"] = rojo.get("nivel", "—")

    def construir_estado_final_v13(
            resultado_azul: dict,
            dorado: dict | None,
            rojo: dict | None,
    ) -> dict:
        """
        v1.3 — ÚNICA salida oficial de la Estrella (panel principal).
        Azul define dirección. Dorado solo aparece si hay ventaja. Rojo verbaliza riesgo cuando Dorado está activo.
        """

        direccion = (resultado_azul.get("direccion") or "NEUTRAL").upper().strip()
        vol = resultado_azul.get("volatilidad_nivel", "Normal")
        score_alcista = int(resultado_azul.get("score_alcista", 0))
        score_bajista = int(resultado_azul.get("score_bajista", 0))
        umbral = int(resultado_azul.get("umbral", 0))

        # 1) Si NO hay Dorado => el estado es AZUL (OBSERVAR)
        if dorado is None:
            return {
                "version": "v1.3",
                "direccion_v13": direccion,
                "volatilidad_nivel": vol,
                "score_alcista": score_alcista,
                "score_bajista": score_bajista,
                "umbral": umbral,

                "esfera": "🔵 Azul (análisis)",
                "decision": "OBSERVAR",
                "mensaje": "No hay ventaja suficiente ahora. Mantente en OBSERVAR.",

                # Solo si quieres el texto por dirección:
                "mensaje_direccion": (
                    "Dirección alcista dominante." if direccion == "ALCISTA"
                    else "Dirección bajista dominante." if direccion == "BAJISTA"
                    else "Dirección neutral: sin ventaja estructural."
                ),

                # Debug opcional (no lo muestres al público)
                "debug_v13": {
                    "dorado_activo": False,
                    "rojo": None,
                }
            }

        # 2) Si hay Dorado => esfera DORADA y decisión Ã¢â‚¬Å“OPERAR CON DISCIPLINAÃ¢â‚¬Â (o la etiqueta que uses)
        riesgo_verbal = None
        if rojo is not None:
            riesgo_verbal = rojo.get("nivel")

        return {
            "version": "v1.3",
            "direccion_v13": direccion,
            "volatilidad_nivel": vol,
            "score_alcista": score_alcista,
            "score_bajista": score_bajista,
            "umbral": umbral,

            "esfera": "🟡 Dorada (criterio y decisión)",
            "decision": "OPERAR CON DISCIPLINA",
            "mensaje": dorado.get("resumen", "Ventaja detectada. Ejecuta con disciplina."),

            # Rojo solo verbal cuando Dorado está activo
            "riesgo": riesgo_verbal or "Moderado",
            "razones_dorado": (dorado.get("razones") or [])[:5],
            "razones_rojo": (rojo.get("razones") or [])[:6] if rojo else [],

            "debug_v13": {
                "dorado_activo": True,
                "micro_score_dorado": dorado.get("micro_score"),
                "rojo": rojo,
            }
        }
    # 4) Enseñar: Premium-only (tu regla final)
    estado["ensenar"] = calcular_contenido_ensenable(
        usuario=usuario,
        estado={
            **estado,
            "decision": estado["accion"],
            "riesgo": estado["riesgo_verbal"],  # sirve como contexto verbal
            "sesion": sesion,
        },
        memoria=memoria
    )

    return estado

from typing import Dict, Any, List

def _fortaleza_direccion(score_alcista: int, score_bajista: int, umbral: int) -> str:
    diff = abs(score_alcista - score_bajista)
    # Fortaleza verbal (simple y estable)
    if diff >= umbral + 2:
        return "fuerte"
    if diff >= umbral:
        return "media"
    return "debil"


def resumen_estado_humano(estado: Dict[str, Any], usuario: Dict[str, Any]) -> Dict[str, Any]:
    """
    Devuelve un resumen humano para debug UI.
    No reemplaza logica; solo presentacion.
    """
    direccion = estado.get("direccion_v13", "NEUTRAL")
    score_a = int(estado.get("score_alcista_v13", estado.get("score_alcista", 0)) or 0)
    score_b = int(estado.get("score_bajista_v13", estado.get("score_bajista", 0)) or 0)
    umbral = int(estado.get("umbral_azul", estado.get("umbral", 0)) or 0)

    fortaleza = _fortaleza_direccion(score_a, score_b, umbral)

    dorado = estado.get("dorado_v13", estado.get("dorado"))
    rojo = estado.get("rojo_v13", estado.get("rojo"))
    ensenar = estado.get("ensenar", None)
    premium = bool((usuario or {}).get("es_premium", False))

    # Dorado
    dorado_activo = bool(dorado) and bool(dorado.get("activo", True)) if isinstance(dorado, dict) else bool(dorado)
    dorado_score = dorado.get("micro_score") if isinstance(dorado, dict) else None
    dorado_umbral = dorado.get("umbral") if isinstance(dorado, dict) else None
    dorado_razones = dorado.get("razones", []) if isinstance(dorado, dict) else []
    dorado_razones = [r for r in dorado_razones if isinstance(r, str) and r.strip()][:2]

    # Rojo
    rojo_nivel = rojo.get("nivel") if isinstance(rojo, dict) else None
    rojo_score = rojo.get("micro_score") if isinstance(rojo, dict) else None
    rojo_razones = rojo.get("razones", []) if isinstance(rojo, dict) else []
    rojo_razones = [r for r in rojo_razones if isinstance(r, str) and r.strip()][:2]

    # Ensenar (premium only)
    ensenar_activo = premium and bool(ensenar)

    return {
        "direccion": {
            "valor": direccion,
            "fortaleza": fortaleza,
            "score_alcista": score_a,
            "score_bajista": score_b,
            "umbral": umbral,
        },
        "dorado": {
            "activo": dorado_activo,
            "micro_score": dorado_score,
            "umbral": dorado_umbral,
            "razones": dorado_razones,
        },
        "rojo": {
            "nivel": rojo_nivel,
            "micro_score": rojo_score,
            "razones": rojo_razones,
        },
        "ensenar": {
            "activo": ensenar_activo,
            "premium": premium,
            "titulo": ensenar.get("titulo") if isinstance(ensenar, dict) else None,
        }
    }
