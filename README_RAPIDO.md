# Estrella Trader V 1.0

## Uso en 30 Segundos
1. Elige mercado (`Cripto` u `Oro`) y `timeframe` en la sidebar.
2. Lee el panel principal:
   - Azul = `OBSERVAR`
   - Dorado + Rojo bajo = posible operacion disciplinada
   - Rojo alto = `NO OPERAR`
3. Pulsa `Actualizar lectura` para refrescar.

## Conectar Telegram (Sin Chat ID Manual)
1. Inicia sesion en la app.
2. En `Cuenta > Telegram`, pulsa `Abrir bot`.
3. En Telegram, toca `Iniciar` (o envia `/start`).
4. Regresa a la app y pulsa `Conectar Telegram`.
5. Si todo sale bien, veras `Telegram conectado`.

Notas:
- Ya no necesitas escribir el `chat_id` manualmente.
- Si falla la conexion, repite `Abrir bot` -> `Iniciar` -> `Conectar Telegram`.
- El bot usado por defecto es `@Estrella_TraderBot`.

## Como Leer Señales
- `Panel principal`: decision actual (`observar` / `operar` / `no operar`).
- `Mensaje Estrella`: explicacion breve del contexto.
- `Dorado`: ventaja tecnica (activo = buena para entrar).
- `Rojo`: riesgo (bajo/controlado = OK; alto/muy alto = evita).

## Mercado Abierto/Cerrado
- La app indica si esta abierto (`live`) o cerrado (`historico`).
- Cerrado: usa velas pasadas, sin precio real en grafico.

## Debug Humano (v1.3)
- Activa en sidebar.
- Muestra direccion dominante (fuerte/media), razones Dorado/Rojo, y `Enseñar` (premium).
- Usalo para entender el "por que", no como señal automatica.

## Mercados Disponibles
- Disponibles hoy: Oro (XAU/USD) y Cripto.

## Errores Comunes
- Confundir velas historicas con live.
- Operar en Azul por ansiedad.
- Ignorar Rojo alto.
- Usar debug como entrada auto.

## Recomendacion
- Filtro primero: `OBSERVAR` (Azul).
- Opera solo si Dorado activo + Rojo aceptable.
- Duda? Espera una vela mas. Disciplina ante todo.
