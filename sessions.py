from datetime import datetime
import pytz

# Zonas horarias comunes (puedes ampliar luego)
ZONAS_HORARIAS = {
    "UTC": "UTC",
    "New York": "America/New_York",
    "Londres": "Europe/London",
    "Bogotá": "America/Bogota",
    "Madrid": "Europe/Madrid"
}

def obtener_hora_actual(zona):
    tz = pytz.timezone(ZONAS_HORARIAS[zona])
    return datetime.now(tz)


def sesion_actual(zona="UTC"):
    hora = obtener_hora_actual(zona).hour

    if 0 <= hora < 7:
        return "Tokio"
    elif 7 <= hora < 13:
        return "Londres"
    elif 13 <= hora < 20:
        return "New York"
    else:
        return "Fuera de sesión"


def calidad_horario(zona="UTC"):
    sesion = sesion_actual(zona)

    if sesion == "Tokio":
        return "baja"
    elif sesion in ["Londres", "New York"]:
        return "alta"
    else:
        return "muy baja"
def explicacion_horario(zona="UTC"):
    sesion = sesion_actual(zona)

    if sesion == "Tokio":
        return "Sesión de Tokio: mercado más lento. Mejor para rangos que para tendencias."
    elif sesion == "Londres":
        return "Sesión de Londres: alta volatilidad. Ideal para rupturas y tendencias."
    elif sesion == "New York":
        return "Sesión de New York: movimientos fuertes, cuidado con sobreoperar."
    else:
        return "Fuera de sesión principal. Riesgo elevado."
