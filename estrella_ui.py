def render_estado_estrella(estado):
    esfera = estado["esfera"]

    if esfera == "ROJO":
        return {
            "emoji": "🔴",
            "color": "red",
            "mensaje": "La estrella entra en modo protección. Prioriza capital.",
            "tono": "protector"
        }

    if esfera == "AZUL":
        return {
            "emoji": "🔵",
            "color": "blue",
            "mensaje": "Condiciones técnicas favorables. Opera con criterio.",
            "tono": "tecnico"
        }

    return {
        "emoji": "🟡",
        "color": "gold",
        "mensaje": "Momento ideal para observar, aprender y afinar lectura.",
        "tono": "mentor"
    }
