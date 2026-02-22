# memoria.py
# =======================
# Memoria de la Estrella Trader
# =======================

# Lista global de recuerdos
RECUERDOS = []  # Cada recuerdo es un dict: {"esfera": str, "tipo": str, "nota": str}
# -----------------------
# ESTRUCTURA BASE DE MEMORIA
# -----------------------

TIPOS_MEMORIA = {
    "observacion": "Memoria observacional (neutra)",
    "aprendizaje": "Memoria de aprendizaje (lección)",
    "error": "Memoria de error (advertencia)"
}
# -----------------------
# NIVELES DE MEMORIA
# -----------------------

MEMORIA_OBSERVACION = "observacion"
MEMORIA_APRENDIZAJE = "aprendizaje"
MEMORIA_ERROR = "error"

def registrar_evento(esfera, tipo, nota, sesion=None):
    """
    Registra un evento en la memoria de la Estrella.

    Args:
        esfera (str): "ROJO", "AZUL" o "DORADO"
        tipo (str): "error", "aprendizaje" "observacion"
        nota (str): descripción del evento
        sesion (str | None): sesión asociada al evento
    """
    global RECUERDOS
    recuerdo = {
        "esfera": esfera,
        "tipo": tipo,
        "nota": nota
    }
    if sesion is not None:
        recuerdo["sesion"] = sesion
    RECUERDOS.append(recuerdo)
# -----------------------
# FORMA ESPERADA DE UN RECUERDO
# -----------------------
"""
Un recuerdo debe ser un dict con al menos:

{
    "tipo": "observacion" | "aprendizaje" | "error",
    "esfera": "azul" | "roja" | "dorada",
    "sesion": "NY" | "Londres" | etc,
    "nota": str
}
"""

def recuerdos_relevantes(esfera):
    """
    Devuelve los recuerdos asociados a una esfera específica.

    Args:
        esfera (str): "ROJO", "AZUL" o "DORADO"

    Returns:
        List[dict]: recuerdos filtrados por esfera
    """
    return [r for r in RECUERDOS if r["esfera"] == esfera]


def limpiar_memoria():
    """
    Limpia todos los recuerdos almacenados.
    """
    global RECUERDOS
    RECUERDOS = []


def advertencia_por_memoria(sesion, esfera=None):
    from memoria import recuerdos_relevantes

    if esfera is None and isinstance(sesion, dict):
        esfera = sesion.get("esfera")

    if not esfera:
        return None

    recuerdos = recuerdos_relevantes(esfera)
    errores = [r for r in recuerdos if r.get("tipo") == "error"]

    if not errores:
        return None

    clasificados = clasificar_errores(errores)

    mensajes = []

    if clasificados["impulsividad"]:
        mensajes.append(
            "⚠️ La Estrella recuerda errores por impulsividad en situaciones similares."
        )

    if clasificados["tecnico"]:
        mensajes.append(
            "🔧 Hay recuerdos de fallos técnicos no confirmados previamente."
        )

    if clasificados["contexto"]:
        mensajes.append(
            "🌪️ El contexto del mercado ya ha causado errores en el pasado."
        )

    if clasificados["desconocido"]:
        mensajes.append(
            "❓ Existen errores previos sin causa claramente identificada."
        )

    if mensajes:
        return "\n".join(mensajes) + "\n\nAvanza con extrema cautela."

    return None


def clasificar_errores(recuerdos):
    """
    Agrupa errores por categoría para interpretación de la Estrella
    """
    clasificados = {
        "impulsividad": [],
        "tecnico": [],
        "contexto": [],
        "desconocido": []
    }

    for r in recuerdos:
        if r.get("tipo") != "error":
            continue

        categoria = r.get("categoria", "desconocido")
        if categoria not in clasificados:
            categoria = "desconocido"

        clasificados[categoria].append(r)

    return clasificados


def nivel_de_memoria(errores):
    """
    Determina el nivel de memoria según cantidad y coherencia de errores
    """
    cantidad = len(errores)

    if cantidad == 1:
        return 1  # Débil
    elif 2 <= cantidad <= 3:
        return 2  # Media
    elif cantidad >= 4:
        return 3  # Dominante

    return 0


def esfera_por_memoria(nivel_memoria):
    """
    Determina qué esfera gana prioridad según la memoria
    """
    if nivel_memoria == 1:
        return "dorado"
    elif nivel_memoria == 2:
        return "azul"
    elif nivel_memoria >= 3:
        return "rojo"
    return None

def influencia_de_memoria(estado):
    """
    Analiza cómo la memoria influye en la decisión actual
    """
    recuerdos = recuerdos_relevantes(estado["esfera"])

    errores = [r for r in recuerdos if r.get("tipo") == "error"]
    aprendizajes = [r for r in recuerdos if r.get("tipo") == "aprendizaje"]
    observaciones = [r for r in recuerdos if r.get("tipo") == "observacion"]

    # Nivel de memoria
    if len(errores) >= 3:
        nivel = 3  # protección
    elif len(errores) >= 1:
        nivel = 2  # advertencia
    elif aprendizajes or observaciones:
        nivel = 1  # leve
    else:
        nivel = 0  # sin memoria

    mensaje = "Protección activa por contexto de mercado."
    if nivel == 3:
        mensaje = "🔴 Memoria de protección activa"
    elif nivel == 2:
        mensaje = "⚠️ Memoria de advertencia"
    elif nivel == 1:
        mensaje = "🟡 Memoria leve de aprendizaje/observación"

    return {
        "nivel": nivel,
        "errores": errores,
        "aprendizajes": aprendizajes,
        "observaciones": observaciones,
        "mensaje": mensaje
    }


