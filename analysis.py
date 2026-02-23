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


def obtener_datos_robusto(ticker: str, period: str = "5d", interval: str = "15m") -> pd.DataFrame:
    """
    Descarga robusta: evita crasheos cuando yfinance devuelve vacÃ­o o falla.
    Retorna DataFrame vacÃ­o si no hay datos.
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
    Toma el Ãºltimo close de velas 1m como proxy de precio live.
    (No es tick real, pero se mueve y sirve para demo visual).
    """
    try:
        df = yf.download(ticker, period="1d", interval="1m", progress=False)
        if df is None or df.empty:
            return None
        return float(df["Close"].iloc[-1])
    except Exception:
        return None



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
# ========= [ENSEÃ‘AR v1.2-C-A - INICIO] =========

def _risk_value(r: str) -> int:
    r = (r or "").lower().strip()
    return {"bajo": 1, "medio": 2, "alto": 3}.get(r, 2)


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def detectar_eventos_mercado(estado: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Detecta SOLO los 4 eventos enseÃ±ables definidos.
    No depende de indicadores extra: usa flags si existen.
    Si no existen, no rompe (solo no detecta).
    """
    eventos: List[Dict[str, Any]] = []

    # Flags recomendadas (si no existen, quedan False)
    ruptura_real = bool(estado.get("ruptura_real", False))
    fallo_ruptura = bool(estado.get("fallo_ruptura", False))
    cambio_tendencia = bool(estado.get("cambio_tendencia", False))
    expansion_volatilidad = bool(estado.get("expansion_volatilidad", False))

    # Contexto mÃ­nimo
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
            # importante: esto es â€œevento mercadoâ€, no â€œevento usuarioâ€
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
            "No toda ruptura continÃºa. Cuando el precio no sostiene, "
            "la confirmaciÃ³n vale mÃ¡s que la velocidad."
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
    Premium-only. Devuelve UNA sola enseÃ±anza (breve, orientadora, sin pasos).
    No aparece si no aporta algo real.
    Formato final:
      { titulo, texto, cierre_opcional, fuente, prioridad }
    """
    if not usuario.get("es_premium", False):
        return None

    decision = (estado.get("decision") or "").upper().strip()
    riesgo = (estado.get("riesgo") or "Medio").capitalize()

    # SeÃ±al â€œestructura vÃ¡lida pero intenciÃ³n no claraâ€ (si no tienes flags, usa texto del estado)
    # Si no existe, no pasa nada.
    estructura_valida = bool(estado.get("estructura_valida", False))
    intencion_clara = bool(estado.get("intencion_clara", True))  # True por defecto

    # Eventos de mercado detectados (si existen flags)
    eventos_mercado = estado.get("eventos_mercado", []) or []

    # Prioridad: 1) ProtecciÃ³n riesgo alto, 2) estructura sin intenciÃ³n, 3) evento mercado relevante
    # MantÃ©n una sola intervenciÃ³n.
    if decision == "NO OPERAR" and _risk_value(riesgo) == 3:
        return {
            "titulo": "ProtecciÃ³n activa",
            "texto": "Evitar operar en riesgo alto protege tu capital y tu calma. "
                     "AquÃ­ la disciplina pesa mÃ¡s que la oportunidad.",
            "cierre": "La Estrella protege primero.",
            "fuente": "decision",
            "prioridad": 1
        }

    if decision == "OBSERVAR" and estructura_valida and not intencion_clara:
        return {
            "titulo": "Estructura sin intenciÃ³n",
            "texto": "La estructura es vÃ¡lida, pero el mercado aÃºn no muestra intenciÃ³n clara. "
                     "Operar aquÃ­ suele venir de anticipaciÃ³n, no de confirmaciÃ³n.",
            "cierre": "Observar tambiÃ©n es operar bien.",
            "fuente": "contexto",
            "prioridad": 2
        }

    # Si hay un evento de mercado, enseÃ±a una idea (sin empujar a operar)
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


# ========= [ENSEÃ‘AR v1.2-C-A - FIN] =========
# ========= [ENSEÃ‘AR - INICIO] ========

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
    v1.2-C-A (Premium fuerte): guÃ­a tipo navegador.
    Devuelve None si:
      - usuario no es premium
      - no hay insight claro
    Devuelve dict con:
      - ahora: acciÃ³n inmediata (1 lÃ­nea)
      - porque: motivo corto (1â€“2 lÃ­neas)
      - proximo: siguiente condiciÃ³n/confirmaciÃ³n (1 lÃ­nea)
      - tono/tipo/prioridad
    """
    if not usuario.get("es_premium", False):
        return None

    decision = _decision_norm(estado.get("decision", ""))
    riesgo = _norm(estado.get("riesgo", "Medio")).capitalize()
    esfera = _norm(estado.get("esfera", ""))
    razones: List[str] = estado.get("razones", []) or []

    # SeÃ±ales mÃ­nimas (si existen en tu estado; si no, quedan False y no rompen)
    sesion = _norm(estado.get("sesion", estado.get("sesiÃ³n", "")))
    estructura_confirmada = bool(estado.get("estructura_confirmada", False))
    rsi = estado.get("rsi", None)  # puede ser float o None
    cerca_ema200 = bool(estado.get("cerca_ema200", False))  # opcional
    cerca_bollinger = bool(estado.get("cerca_bollinger", False))  # opcional

    # Memoria (si existe)
    impulsividad = _bool_from_memoria(memoria, "indica_impulsividad", False)
    sobreoperar_sesion_baja = _bool_from_memoria(memoria, "sobreoperar_sesion_baja", False)
    entrar_sin_confirmacion = _bool_from_memoria(memoria, "entrar_sin_confirmacion", False)

    # --- Motor de reglas (cada regla produce una "guÃ­a" corta) ---
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

    # REGLA CORE â€” Estructura vÃ¡lida pero intenciÃ³n no clara (navegador puro)
    estructura_valida = any(
        "estructura" in (r or "").lower()
        and ("vÃ¡lida" in (r or "").lower() or "valida" in (r or "").lower())
        for r in razones
    )
    intencion_no_clara = (
            "intenciÃ³n" in (estado.get("frase_pedagogica", "") or "").lower()
            and "no clara" in (estado.get("frase_pedagogica", "") or "").lower()
    )

    if decision == "OBSERVAR" and estructura_valida and intencion_no_clara:
        candidatos.append(guia(
            titulo="ðŸ§­ Guia de la Estrella",
            ahora="Ahora: Mantente en OBSERVAR.",
            porque="La estructura es valida, pero todavia no hay intencion. Entrar aqui es adivinar.",
            proximo="Proximo paso: espera confirmacn (cierre claro + ruptura/rechazo) antes de considerar operar.",
            tipo="lectura",
            prioridad=2,
            etiqueta="estructura_ok_intencion_no_clara",
        ))

    # REGLA 1 â€” Fuera de sesiÃ³n + observar: guÃ­a protectora (muy â€œnavegadorâ€)
    if sesion.lower().startswith("fuera") and decision in ("OBSERVAR", "NO OPERAR"):
        candidatos.append(guia(
            titulo="ðŸ§­ GuÃ­a de la Estrella",
            ahora="Ahora: Mantente en OBSERVAR.",
            porque="Fuera de sesiÃ³n la calidad baja y el ruido sube. No hay prisa.",
            proximo="PrÃ³ximo paso: vuelve en sesiÃ³n principal y busca confirmaciÃ³n limpia.",
            tipo="proteccion",
            prioridad=1,
            etiqueta="fuera_sesion_proteccion",
        ))

    # REGLA 2 â€” OBSERVAR + riesgo medio/alto + impulsividad: prevenciÃ³n personalizada
    if decision == "OBSERVAR" and _risk_value(riesgo) >= 2 and impulsividad:
        candidatos.append(guia(
            titulo="ðŸ§­ GuÃ­a de la Estrella",
            ahora="Ahora: Observa sin intervenir.",
            porque="Este es el contexto donde tu mente suele apurarse. Hoy el control es ventaja.",
            proximo="PrÃ³ximo paso: espera una vela de confirmaciÃ³n antes de considerar operar.",
            tipo="disciplina",
            prioridad=1,
            etiqueta="observar_impulsividad",
        ))

    # REGLA 3 â€” NO OPERAR + riesgo alto: refuerzo simple (sin medidor)
    if decision == "NO OPERAR" and _risk_value(riesgo) == 3:
        candidatos.append(guia(
            titulo="ðŸ§­ GuÃ­a de la Estrella",
            ahora="Ahora: NO OPERAR.",
            porque="El riesgo alto no se negocia. Proteger capital tambiÃ©n es progreso.",
            proximo="PrÃ³ximo paso: espera que el riesgo baje o que el contexto se ordene.",
            tipo="proteccion",
            prioridad=1,
            etiqueta="no_operar_riesgo_alto",
        ))

    # REGLA 4 â€” OBSERVAR + RSI neutral + entrar sin confirmaciÃ³n (memoria): â€œno te adelantesâ€ con razÃ³n personal
    if decision == "OBSERVAR" and rsi is not None and 40 <= float(rsi) <= 60 and entrar_sin_confirmacion:
        candidatos.append(guia(
            titulo="ðŸ§­ GuÃ­a de la Estrella",
            ahora="Ahora: OBSERVAR en neutral.",
            porque="En RSI neutral tu error tÃ­pico es anticipar. AquÃ­ se pierde por impaciencia.",
            proximo="PrÃ³ximo paso: espera salida de neutral + estructura clara.",
            tipo="correccion",
            prioridad=2,
            etiqueta="rsi_neutral_memoria",
        ))

    # REGLA 5 â€” OBSERVAR + cerca EMA200: guÃ­a de lectura (tÃ©cnico, sobrio)
    if decision == "OBSERVAR" and cerca_ema200:
        candidatos.append(guia(
            titulo="ðŸ§­ GuÃ­a de la Estrella",
            ahora="Ahora: Lee reacciÃ³n en EMA 200.",
            porque="EMA 200 suele actuar como zona de decisiÃ³n. No se adivina: se observa.",
            proximo="PrÃ³ximo paso: confirma rechazo/ruptura con velas antes de actuar.",
            tipo="lectura",
            prioridad=3,
            etiqueta="ema200_lectura",
        ))

    # REGLA 6 â€” OBSERVAR + cerca Bollinger: evitar entradas por â€œtoqueâ€
    if decision == "OBSERVAR" and cerca_bollinger:
        candidatos.append(guia(
            titulo="ðŸ§­ GuÃ­a de la Estrella",
            ahora="Ahora: No operes por tocar banda.",
            porque="Bollinger no es seÃ±al por sÃ­ sola. El contexto manda, no el borde.",
            proximo="PrÃ³ximo paso: espera compresiÃ³n/expansiÃ³n + confirmaciÃ³n.",
            tipo="lectura",
            prioridad=3,
            etiqueta="bollinger_lectura",
        ))

    # REGLA 7 â€” OPERAR + estructura confirmada + riesgo bajo: guÃ­a de ejecuciÃ³n responsable
    if decision == "OPERAR" and estructura_confirmada and _risk_value(riesgo) == 1:
        candidatos.append(guia(
            titulo="ðŸ§­ GuÃ­a de la Estrella",
            ahora="Ahora: Ejecuta con disciplina.",
            porque="La ventaja no estÃ¡ en entrar, estÃ¡ en gestionar el riesgo y respetar el plan.",
            proximo="PrÃ³ximo paso: define invalidaciÃ³n y tamaÃ±o antes de la entrada.",
            tipo="ejecucion",
            prioridad=2,
            etiqueta="operar_disciplina",
        ))

    # REGLA 8 â€” OBSERVAR + riesgo medio/alto + sobreoperar_sesion_baja: aviso fino (sin sermÃ³n)
    if decision == "OBSERVAR" and _risk_value(riesgo) >= 2 and sobreoperar_sesion_baja:
        candidatos.append(guia(
            titulo="ðŸ§­ GuÃ­a de la Estrella",
            ahora="Ahora: MantÃ©n la calma.",
            porque="En horarios de baja calidad tu historial muestra sobreoperaciÃ³n.",
            proximo="PrÃ³ximo paso: limita intentos o espera mejor sesiÃ³n.",
            tipo="proteccion",
            prioridad=2,
            etiqueta="sesion_baja_sobreoperar",
        ))

    if not candidatos:
        return None

    # Elegir el mÃ¡s importante (prioridad menor = mÃ¡s importante)
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
        resumen = "No hay direcciÃ³n dominante suficiente."
    return {
        "titulo": elegido["titulo"],
        "ahora": elegido["ahora"],
        "porque": elegido["porque"],
        "proximo": elegido["proximo"],
        "tipo": elegido["tipo"],
        "prioridad": elegido["prioridad"],
        "etiqueta": elegido["etiqueta"],
        # Ãºtil para debug, pero si no quieres ruido, no lo muestres
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
    # Premium "EnseÃ±ar" (guÃ­a tipo navegador)
    estado["ensenar"] = calcular_contenido_ensenable(
        usuario=usuario,
        estado=estado,
        memoria=memoria  # si no tienes memoria aquÃ­, pasa None
    )
    if "rsi" in estado:
        try:
            estado["rsi_actual"] = round(float(estado["rsi"]), 2)
        except (TypeError, ValueError):
            pass

    return estado


# ========= [ENSEÃ‘AR - FIN] =========


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

    # AsignaciÃ³n segura (independiente de versiÃ³n)
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
        return "compresiÃ³n"
    elif close > bbu:
        return "ruptura_alcista"
    elif close < bbl:
        return "ruptura_bajista"
    else:
        return "normal"


def interpretar_rsi(rsi):
    if rsi > 70:
        return "El RSI estÃ¡ en sobrecompra. Entrar aquÃ­ suele ser riesgoso."
    elif rsi < 30:
        return "El RSI estÃ¡ en sobreventa. Puede haber rebote, pero confirma."
    else:
        return "El RSI estÃ¡ en zona neutral. Espera estructura o confirmaciÃ³n."


def advertencia_trade(tendencia, estado_rsi):
    if estado_rsi == "sobrecompra" and tendencia == "alcista":
        return "âš ï¸ Precio extendido. Espera retroceso."
    if estado_rsi == "sobreventa" and tendencia == "bajista":
        return "âš ï¸ PresiÃ³n bajista fuerte. No anticipes."
    return "âœ… Contexto sano. Observa estructura antes de entrar."


def estructura_mercado(data):
    data = data.copy()

    # ProtecciÃ³n por si las columnas vienen raras
    columnas = list(data.columns)

    if "High" not in columnas or "Low" not in columnas or "Close" not in columnas:
        return "Estructura no disponible âš ï¸"

    max_20 = data["High"].rolling(window=20).max().iloc[-1]
    min_20 = data["Low"].rolling(window=20).min().iloc[-1]
    precio = data["Close"].iloc[-1]

    if precio > max_20:
        return "Ruptura alcista ðŸ“ˆ"
    elif precio < min_20:
        return "Ruptura bajista ðŸ“‰"
    else:
        return "Rango / consolidaciÃ³n â¸ï¸"


def evaluar_estado_estrella(datos, sesion, calidad):
    """
    EvalÃºa el estado interno de la Estrella Trader
    y decide quÃ© esfera lidera segÃºn el contexto real del mercado
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

    # SesiÃ³n lenta
    if sesion == "Tokio":
        condiciones_rojas.append("SesiÃ³n Tokio (baja intenciÃ³n)")
    # Fuera de sesi?n principal
    if sesion == "Fuera de sesi?n":
        condiciones_rojas.append("Fuera de sesi?n principal")

    # Horario malo
    if calidad == "baja":
        condiciones_rojas.append("Horario de baja calidad")

    # ðŸ”´ ROJO FUERTE
    if len(condiciones_rojas) >= 2:
        return {
            "esfera": "ðŸ”´ Roja (protecciÃ³n total)",
            "riesgo": "Muy alto",
            "accion": "No operar",
            "razones": condiciones_rojas,  # âœ… AÃ‘ADIR
            "mensaje": (
                    "La Estrella activa protecciÃ³n total.\n\n"
                    "El mercado no estÃ¡ en un estado saludable:\n- "
                    + "\n- ".join(condiciones_rojas) +
                    "\n\nPreservar capital tambiÃ©n es una decisiÃ³n profesional."
            )
        }

    # ðŸ”´ ROJO SUAVE
    if len(condiciones_rojas) == 1:
        return {
            "esfera": "ðŸ”´ Roja (precauciÃ³n)",
            "riesgo": "Alto",
            "accion": "Esperar o reducir tamaÃ±o",
            "razones": condiciones_rojas,  # âœ… AÃ‘ADIR
            "mensaje": (
                    "Hay una seÃ±al de advertencia en el mercado:\n- "
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
            "Estructura tÃ©cnica vÃ¡lida"
        ]
        return {
            "esfera": "ðŸ”µ Azul (anÃ¡lisis)",
            "riesgo": "Medio",
            "accion": "Observar estructura",
            "razones": razones_azules,  # âœ… AÃ‘ADIR
            "mensaje": (
                "El mercado muestra orden tÃ©cnico, pero aÃºn no hay intenciÃ³n clara.\n"
                "Lee las velas, no te adelantes.\n"
                "La paciencia tambiÃ©n es una decisiÃ³n."
            )
        }

    if pd.isna(precio) or pd.isna(ema_200):
        sesgo = "neutral"
    else:
        sesgo = "alcista" if precio > ema_200 else "bajista"
    razones_doradas = [
        "SesiÃ³n favorable",
        "Tendencia definida",
        "Riesgo bajo"
    ]

    return {
        "esfera": "ðŸŸ¡ Dorada (criterio y decisiÃ³n)",
        "riesgo": "Bajo",
        "accion": "Posible entrada consciente",
        "razones": razones_doradas,
        "mensaje": (
            "El contexto acompaÃ±a.\n"
            f"SesiÃ³n favorable con sesgo {sesgo}.\n\n"
            "Decide con paciencia y gestiÃ³n de riesgo."
        )
    }


import numpy as np


def calcular_score_azul(data: pd.DataFrame) -> dict:
    """
    v1.3 â€” NÃºcleo Azul
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
    # 1ï¸âƒ£ ESTRUCTURA (Peso mayor)
    # =========================
    score_alcista = 0
    score_bajista = 0

    highs = data["High"].rolling(5).max()
    lows = data["Low"].rolling(5).min()

    # Ãšltimos valores
    if highs.iloc[-1] > highs.iloc[-2] and lows.iloc[-1] > lows.iloc[-2]:
        score_alcista += 3

    if highs.iloc[-1] < highs.iloc[-2] and lows.iloc[-1] < lows.iloc[-2]:
        score_bajista += 3

    # =========================
    # 2ï¸âƒ£ EMAs
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
    # 3ï¸âƒ£ RSI
    # =========================
    rsi = data["RSI"].iloc[-1]

    if not pd.isna(rsi):
        if rsi > 55:
            score_alcista += 1
        elif rsi < 45:
            score_bajista += 1

    # =========================
    # 4ï¸âƒ£ VOLATILIDAD (ATR)
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
    # 5ï¸âƒ£ DIRECCIÃ“N DOMINANTE
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
        return "Prefiero que hoy no arriesgues. El mercado no estÃ¡ claro."

    if tono == "tecnico":
        return "Las estructuras se alinean. Ejecuta solo si confirmas."

    if "AZUL" in esfera or "Azul" in esfera:
        return (
            "ðŸŒŸ Estrella:\n"
            "Observa con atenciÃ³n.\n\n"
            f"{estado['mensaje']}\n\n"
            "El anÃ¡lisis paciente es una ventaja que pocos usan."
        )

    if "DORADO" in esfera or "Dorada" in esfera:
        return (
            "ðŸŒŸ Estrella:\n"
            "Este es un buen contexto.\n\n"
            f"{estado['mensaje']}\n\n"
            "Recuerda: una buena entrada empieza con una buena decisiÃ³n."
        )

    # Fallback
    return "ðŸŒŸ Estrella:\nEstoy evaluando el mercado."


def voz_estrella_con_memoria(estado, recuerdos):
    """
    Modula la voz de la Estrella segÃºn la influencia de la memoria
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
                "\n\nðŸ§  La Estrella recuerda experiencias previas similares. "
                "Observa con atenciÃ³n."
        )  # muy bien

    if nivel == 2:
        return (
                mensaje_base +
                "\n\nâš ï¸ Advertencia de memoria: este contexto ha generado errores antes. "
                "Reduce riesgo y confirma mÃ¡s de lo normal."
        )

    if nivel == 3:
        return (
            "ðŸ›‘ Memoria de protecciÃ³n activa.\n\n"
            "La Estrella ha visto pÃ©rdidas repetidas en este contexto.\n"
            "No es recomendable operar ahora."
        )

    return mensaje_base


def estado_estrella(datos, sesion, calidad):
    """
    v1.3 â€” Estado basado en NÃºcleo Azul
    """

    resultado_azul = calcular_score_azul(datos)
    direccion = resultado_azul["direccion"]

    if direccion == "ALCISTA":
        return {
            "esfera": "ðŸ”µ Azul (anÃ¡lisis)",
            "riesgo": "Evaluando...",
            "accion": "Esperando ventaja",
            "mensaje": "DirecciÃ³n alcista dominante."
        }

    elif direccion == "BAJISTA":
        return {
            "esfera": "ðŸ”µ Azul (anÃ¡lisis)",
            "riesgo": "Evaluando...",
            "accion": "Esperando ventaja",
            "mensaje": "DirecciÃ³n bajista dominante."
        }

    else:
        return {
            "esfera": "ðŸ”µ Azul (anÃ¡lisis)",
            "riesgo": "Neutral",
            "accion": "Observar",
            "mensaje": (
                "Neutral real: el mercado no ofrece ventaja clara.\n"
                "Espera confirmaciÃ³n estructural."
            )
        }



import numpy as np
from typing import Dict, Any, Optional


def _atr14(df: pd.DataFrame) -> float:
    """ATR(14) simple para umbrales adaptativos."""
    d = df.copy()
    tr = np.maximum(
        d["High"] - d["Low"],
        np.maximum(
            (d["High"] - d["Close"].shift()).abs(),
            (d["Low"] - d["Close"].shift()).abs()
        )
    )
    atr = pd.Series(tr).rolling(14).mean()
    val = float(atr.iloc[-1]) if not pd.isna(atr.iloc[-1]) else 0.0
    return max(val, 1e-9)


def _nivel_resistencia(df: pd.DataFrame, n: int = 50) -> float:
    """Resistencia simple: mÃ¡ximo reciente."""
    return float(df["High"].tail(n).max())


def _nivel_soporte(df: pd.DataFrame, n: int = 50) -> float:
    """Soporte simple: mÃ­nimo reciente."""
    return float(df["Low"].tail(n).min())


def calcular_micro_score_dorado(df: pd.DataFrame, direccion: str) -> Optional[Dict[str, Any]]:
    """
    v1.3 â€” Dorado (micro-score de ventaja)
    - Solo evalÃºa SI ya hay direcciÃ³n dominante (ALCISTA/BAJISTA).
    - Devuelve None si no hay ventaja suficiente.
    """

    direccion = (direccion or "").upper().strip()
    if direccion not in ("ALCISTA", "BAJISTA"):
        return None

    d = df.copy()
    close = float(d["Close"].iloc[-1])
    ema20 = float(d["EMA_20"].iloc[-1])
    ema50 = float(d["EMA_50"].iloc[-1])
    atr = _atr14(d)

    # =========================
    # Micro-score Dorado (0â€“7)
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

    # 2) Zona tÃ©cnica simple (proximidad a soporte/resistencia reciente)
    soporte = _nivel_soporte(d, n=50)
    resistencia = _nivel_resistencia(d, n=50)

    # distancia relativa en ATR
    dist_soporte = abs(close - soporte) / atr
    dist_resistencia = abs(resistencia - close) / atr

    if direccion == "ALCISTA":
        # ventaja si estÃ¡ mÃ¡s cerca de soporte que de resistencia
        if dist_soporte <= 1.2 and dist_resistencia >= 1.2:
            score += 2
            razones.append("Precio en zona favorable (mÃ¡s cerca de soporte que de resistencia).")
        elif dist_soporte <= 1.2:
            score += 1
            razones.append("Precio cerca de soporte reciente (zona interesante).")
    else:  # BAJISTA
        if dist_resistencia <= 1.2 and dist_soporte >= 1.2:
            score += 2
            razones.append("Precio en zona favorable (mÃ¡s cerca de resistencia que de soporte).")
        elif dist_resistencia <= 1.2:
            score += 1
            razones.append("Precio cerca de resistencia reciente (zona interesante).")

    # 3) RR mÃ­nimo estimado (simple con ATR)
    # Idea: si direcciÃ³n alcista, objetivo "hacia resistencia"; si bajista, "hacia soporte".
    # Stop aproximado: 1 ATR.
    stop_atr = 1.0
    if direccion == "ALCISTA":
        target_atr = max((resistencia - close) / atr, 0.0)
    else:
        target_atr = max((close - soporte) / atr, 0.0)

    rr = (target_atr / stop_atr) if stop_atr > 0 else 0.0

    if rr >= 2.0:
        score += 2
        razones.append("RR estimado >= 1:2 (ventaja de ejecuciÃ³n).")
    elif rr >= 1.5:
        score += 1
        razones.append("RR estimado aceptable (~1:1.5).")

    # Umbral Dorado (activaciÃ³n)
    # =========================
    # Umbral Dorado adaptativo (no regalar)
    # =========================
    # Si el mercado estÃ¡ muy volÃ¡til, exigimos mÃ¡s (evita falsas "ventajas")
    # Si estÃ¡ normal, umbral estÃ¡ndar
    # Si estÃ¡ calmado, permitimos ventaja parcial pero aÃºn estricta
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
        accion = "Posible preparaciÃ³n alcista"
        resumen = f"Ventaja alcista detectada (micro-score {score}/{umbral_final})"
    else:
        accion = "Posible preparaciÃ³n bajista"
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
    v1.3 â€” Rojo (micro-score de riesgo acumulativo)
    - No bloquea
    - Devuelve nivel verbal
    - Idealmente se usa cuando Dorado estÃ¡ activo (hay posible ejecuciÃ³n)
    """

    direccion = (direccion or "").upper().strip()
    d = df.copy()

    close = float(d["Close"].iloc[-1])
    ema20 = float(d["EMA_20"].iloc[-1])

    # ATR para umbrales adaptativos
    atr = _atr14(d)

    # 1) Volatilidad extrema (ATR ratio)
    # Reutilizamos ATR(14) y un proxy con ATR(50) calculado rÃ¡pido
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

    # 2) ExtensiÃ³n excesiva respecto EMA20 (en ATR)
    ext = abs(close - ema20) / atr
    if ext > 2.2:
        score += 3
        razones.append("Precio muy extendido respecto a EMA20.")
    elif ext > 1.8:
        score += 2
        razones.append("Precio extendido respecto a EMA20.")

    # 3) CercanÃ­a a zona estructural â€œduraâ€
    soporte = _nivel_soporte(d, n=50)
    resistencia = _nivel_resistencia(d, n=50)

    dist_soporte = abs(close - soporte) / atr
    dist_resistencia = abs(resistencia - close) / atr

    # En alcista: riesgo si estÃ¡s pegado a resistencia
    if direccion == "ALCISTA" and dist_resistencia <= 0.8:
        score += 2
        razones.append("CercanÃ­a a resistencia fuerte (posible rechazo).")

    # En bajista: riesgo si estÃ¡s pegado a soporte
    if direccion == "BAJISTA" and dist_soporte <= 0.8:
        score += 2
        razones.append("CercanÃ­a a soporte fuerte (posible rebote).")

    # 4) Memoria (por ahora manual / 0 mientras no tengamos SQLite)
    # impacto_memoria puede ser: 0, -1, -2 (negativa), +1 (positiva)
    if impacto_memoria <= -2:
        score += 3
        razones.append("Memoria histÃ³rica muy negativa en contexto similar.")
    elif impacto_memoria == -1:
        score += 2
        razones.append("Memoria negativa en contexto similar.")

    # 5) Divergencia RSI (simple proxy)
    # Si RSI estÃ¡ cayendo mientras precio sube (o viceversa) en Ãºltimas velas
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
        "esfera": "ðŸ”µ Azul (anÃ¡lisis)",
        "decision": "OBSERVAR",
        "accion": "OBSERVAR",
        "riesgo": "Bajo",
        "mensaje": "No hay ventaja suficiente ahora. Mantente en OBSERVAR.",
        "frase_pedagogica": "No hay ventaja suficiente ahora. Mantente en OBSERVAR.",
        "mensaje_direccion": (
            "DirecciÃ³n alcista dominante." if direccion == "ALCISTA"
            else "DirecciÃ³n bajista dominante." if direccion == "BAJISTA"
            else "DirecciÃ³n neutral: sin ventaja estructural."
        ),
    }

    if dorado is not None:
        estado.update({
            "esfera": "ðŸŸ¡ Dorada (criterio y decisiÃ³n)",
            "decision": "OPERAR CON DISCIPLINA",
            "accion": dorado.get("accion", "Posible ventaja"),
            "riesgo": (rojo or {}).get("nivel", "Moderado"),
            "mensaje": dorado.get("resumen", "Ventaja detectada. Ejecuta con disciplina."),
            "frase_pedagogica": dorado.get("resumen", "Ventaja detectada. Ejecuta con disciplina."),
        })

    if rojo is not None and rojo.get("nivel") in ("Alto", "Muy alto"):
        estado.update({
            "esfera": "ðŸ”´ Roja (riesgo)",
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
        return "Este contexto ya ha causado pÃ©rdidas antes."
    if "azul" in esfera_min or esfera_min == "azul":
        return "Contexto repetido de operaciones tÃ©cnicas."
    return "Buen momento histÃ³rico para observar patrones."


# âš ï¸ NOTA:
# Esta funciÃ³n pertenece a la capa UI.
# Se mantiene aquÃ­ temporalmente para estabilidad.
def render_estado_estrella(estado):
    """
    Traduce el estado interno de la estrella a UI (emoji + mensaje)
    """

    if estado["esfera"] == "ROJA":
        return {
            "emoji": "ðŸ”´",
            "mensaje": "Modo protecciÃ³n â€” la estrella recomienda no operar"
        }

    if estado["esfera"] == "AZUL":
        return {
            "emoji": "ðŸ”µ",
            "mensaje": "Modo operativo â€” condiciones tÃ©cnicas favorables"
        }

    return {
        "emoji": "ðŸŸ¡",
        "mensaje": "Modo anÃ¡lisis â€” observa y aprende del mercado"
    }
    # --- LÃ“GICA FUTURA (ACTUALMENTE INALCANZABLE) ---
    # Este bloque queda documentado para evoluciÃ³n futura


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
    Devuelve los recuerdos relevantes segÃºn la esfera dominante
    """
    from memoria import recuerdos_relevantes  # Importa aquÃ­ para evitar ciclos
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
    Decide quÃ© hacer segÃºn estado actual y memoria
    """

    if estado["esfera"].startswith("ðŸ”´"):
        return {
            "decision": "NO OPERAR",
            "motivo": "Riesgo elevado detectado por la Estrella."
        }

    if influencia["errores"]:
        return {
            "decision": "NO OPERAR",
            "motivo": "La memoria recuerda errores en contextos similares."
        }

    if estado["esfera"].startswith("ðŸŸ¡") and estado["riesgo"].lower() == "bajo":
        return {
            "decision": "OPERAR CON DISCIPLINA",
            "motivo": "Contexto favorable con riesgo controlado."
        }

    return {
        "decision": "OBSERVAR",
        "motivo": "No hay confirmaciÃ³n suficiente."
    }

# ----------------------------------
# ESTRELLA TRADER v1.1 â€” ARQUITECTURA CONGELADA
# A partir de aquÃ­:
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
    V1.3 â€” Estado Final Unificado:
    Azul define direcciÃ³n â†’ Dorado decide si hay ventaja â†’ Rojo mide riesgo.
    Devuelve un solo dict listo para UI.
    """

    # 1) Azul: direcciÃ³n dominante
    azul = calcular_score_azul(datos)
    direccion = azul["direccion"]

    estado = {
        "version": "v1.3",
        "direccion": direccion,          # ALCISTA / BAJISTA / NEUTRAL
        "azul": azul,                    # debug opcional
        "dorado": None,
        "rojo": None,
        "esfera": "ðŸ”µ Azul (anÃ¡lisis)",
        "riesgo_verbal": "â€”",
        "accion": "OBSERVAR",
    }

    # 2) Dorado: SOLO si Azul NO es neutral
    if direccion in ("ALCISTA", "BAJISTA"):
        dorado = calcular_micro_score_dorado(datos, direccion)
        estado["dorado"] = dorado

        if dorado is not None:
            # Dorado activo â†’ cambia esfera a ðŸŸ¡
            estado["esfera"] = "ðŸŸ¡ Dorada (ventaja)"
            estado["accion"] = "PREPARAR"

            # 3) Rojo: solo si Dorado estÃ¡ activo (para medir riesgo de ejecuciÃ³n)
            rojo = calcular_micro_score_rojo(datos, direccion, dorado=dorado, impacto_memoria=0)
            estado["rojo"] = rojo
            estado["riesgo_verbal"] = rojo.get("nivel", "â€”")

    def construir_estado_final_v13(
            resultado_azul: dict,
            dorado: dict | None,
            rojo: dict | None,
    ) -> dict:
        """
        v1.3 â€” ÃšNICA salida oficial de la Estrella (panel principal).
        Azul define direcciÃ³n. Dorado solo aparece si hay ventaja. Rojo verbaliza riesgo cuando Dorado estÃ¡ activo.
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

                "esfera": "ðŸ”µ Azul (anÃ¡lisis)",
                "decision": "OBSERVAR",
                "mensaje": "No hay ventaja suficiente ahora. Mantente en OBSERVAR.",

                # Solo si quieres el texto por direcciÃ³n:
                "mensaje_direccion": (
                    "DirecciÃ³n alcista dominante." if direccion == "ALCISTA"
                    else "DirecciÃ³n bajista dominante." if direccion == "BAJISTA"
                    else "DirecciÃ³n neutral: sin ventaja estructural."
                ),

                # Debug opcional (no lo muestres al pÃºblico)
                "debug_v13": {
                    "dorado_activo": False,
                    "rojo": None,
                }
            }

        # 2) Si hay Dorado => esfera DORADA y decisiÃ³n â€œOPERAR CON DISCIPLINAâ€ (o la etiqueta que uses)
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

            "esfera": "ðŸŸ¡ Dorada (criterio y decisiÃ³n)",
            "decision": "OPERAR CON DISCIPLINA",
            "mensaje": dorado.get("resumen", "Ventaja detectada. Ejecuta con disciplina."),

            # Rojo solo verbal cuando Dorado estÃ¡ activo
            "riesgo": riesgo_verbal or "Moderado",
            "razones_dorado": (dorado.get("razones") or [])[:5],
            "razones_rojo": (rojo.get("razones") or [])[:6] if rojo else [],

            "debug_v13": {
                "dorado_activo": True,
                "micro_score_dorado": dorado.get("micro_score"),
                "rojo": rojo,
            }
        }
    # 4) EnseÃ±ar: Premium-only (tu regla final)
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
