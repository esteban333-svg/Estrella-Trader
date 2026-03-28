# Ideas pendientes

## precision-alta-sesgo-confirmacion

Fecha: 2026-03-20

Objetivo: subir el porcentaje de acierto del proyecto sin romper la logica actual.

Resumen:
- Separar `direccion` de `entrada`.
- `Direccion` solo define el sesgo: alcista = buscar largos, bajista = buscar cortos.
- La alerta operativa solo sale con confirmacion real.
- Endurecer filtros de precision: mas confianza minima, mas persistencia, confirmacion de price action y mas validacion multi-timeframe.
- Bloquear o degradar senales debiles: si la fuerza es debil o no hay patron limpio, dejarla como prealerta y no como entrada inmediata.
- Operar solo a favor de estructura mayor.
- Reducir ruido con menos activos o menos temporalidades de baja calidad.
- Priorizar simbolos y timeframes con mejor rendimiento real guardado en `scanner_state.json`.

Idea de implementacion:
- Crear un modo de trabajo llamado `precision alta`.
- Mantener `direccion` como sesgo.
- Agregar una capa final de `confirmacion de entrada`.
- Enviar alerta completa solo cuando sesgo + confirmacion + filtros pasen juntos.



## mentor-pedagogico-alerta-compacta

Fecha: 2026-03-21

Objetivo: hacer las alertas mas claras, menos ruidosas y mas utiles para aprender a operar.

Resumen:
- Compactar la alerta principal para dejar solo lo operativo: direccion, accion, entrada, SL, TP, fuerza, puntaje tecnico y timeframe.
- Mantener siempre el bloque del `Mentor`.
- Hacer que el `Mentor` deje de sonar como una advertencia corta y pase a explicar la lectura del setup.
- El mensaje del mentor debe ensenar: que ve el sistema, que falta para entrar, cual es el riesgo y cual es el error comun del trader.
- Tratar alertas con `fuerza debil`, `sin_patron` o confirmacion incompleta como contexto guiado o prealerta, no como entrada agresiva.

Idea de implementacion:
- Separar `alerta corta` y `lectura del mentor`.
- Renombrar `Confianza` a `Puntaje tecnico` para evitar confusion.
- Renombrar `Score` a `Checklist tecnico` o `Validacion tecnica`.
- Definir una estructura fija para el mentor:
  - `Lectura`
  - `Riesgo`
  - `Disciplina`
  - `Error comun`
- Priorizar lenguaje pedagogico: breve, claro y profundo, no solo operativo.

Formato propuesto de alerta Telegram:

```text
[Estrella Trader] XRP-USD | 1D + 4H

Direccion: Bajista
Accion: Esperar confirmacion
Probabilidad operativa: Media
Tipo de escenario: Posible pullback de continuacion
Entrada guia: 1.4379
SL guia: sobre el ultimo maximo valido
TP guia: siguiente expansion bajista / 3R
Fuerza: Debil
Puntaje tecnico: 83/100
Entorno: Ruidoso

Mentor:
Lectura: el sesgo es bajista, pero la estructura todavia no entrega una entrada limpia.
Lo mas probable es un pullback antes de continuar, no una caida inmediata sin retroceso.
Confirmacion: verifica rechazo en retesteo, continuidad bajista y defensa de la estructura.
Invalidacion: si recupera estructura y sostiene arriba, la idea pierde valor.
Error comun: vender por impulso solo porque la direccion dice bajista.
Trader disciplinado: espera confirmacion; no persigue precio.
```

Y si fuera una senal mucho mas solida, sonaria asi:

```text
[Estrella Trader] ADA-USD | 30m

Direccion: Bajista
Accion: Preparar venta
Probabilidad operativa: Alta
Tipo de escenario: Continuacion tendencial confirmada
Entrada guia: 0.2644
SL guia: sobre el retesteo
TP guia: proyeccion 3R
Fuerza: Fuerte
Puntaje tecnico: 88/100
Entorno: Limpio

Mentor:
Lectura: la tendencia mantiene presion bajista y la estructura sigue ordenada.
Aqui la ventaja no esta en anticipar, sino en vender donde el mercado confirma continuacion.
Confirmacion: busca retesteo fallido o nueva vela de impulso con rechazo comprador debil.
Invalidacion: si rompe el ultimo techo operativo, la continuacion pierde fuerza.
Error comun: entrar tarde, cuando ya se extendio el movimiento.
Trader disciplinado: ejecuta solo si el gatillo aparece en zona valida.
```

Estructura fija ideal:
- encabezado corto
- bloque operativo
- bloque mentor

Asi Telegram sigue siendo rapido de leer, pero el mentor si ensena.



## medicion-historica-worker

Fecha: 2026-03-21

Objetivo: medir historicamente el rendimiento real del worker y separar puntaje tecnico de precision comprobada.

Resumen:
- Medir el worker con resultados historicos reales de cada alerta: `win`, `loss`, `timeout` y `replaced`.
- Mostrar precision real global del worker, no solo confianza o puntaje tecnico de la alerta actual.
- Separar claramente:
  - `Puntaje tecnico actual`
  - `Precision historica real`
  - `Riesgo / ruido`
- Calcular rendimiento por activo, por timeframe y por tipo de setup.
- Poder responder preguntas como:
  - cuantas de 100 ganaria el worker hoy
  - cuantas perderia
  - en que activos o timeframes es realmente fuerte

Idea de implementacion:
- Construir un panel historico del worker con:
  - precision global
  - wins / losses / timeouts
  - RR promedio
  - score operativo 1-100
  - ranking por activo
  - ranking por timeframe
  - ranking por setup
- Cruzar resultados historicos con variables como:
  - `direccion`
  - `fuerza`
  - `patron`
  - `puntaje tecnico`
  - `timeframe`
- Usar esa informacion para decir no solo si una alerta suena bien, sino si historicamente ese tipo de alerta realmente funciona.


## NO ES IDEA SOLO ES PARA MI 
## escala-puntaje-probabilidad-alertas

Fecha: 2026-03-21

Objetivo: traducir el puntaje tecnico del worker a una lectura humana simple de probabilidad aproximada de ganar y volumen esperado de alertas.

Resumen:
- Crear una escala orientativa para que el usuario entienda mejor que significa un puntaje como `72`, `74`, `76`, `78` o `80`.
- Mostrar la probabilidad aproximada de ganar como una lectura humana, no como una precision historica exacta.
- Relacionar tambien cada rango de puntaje con la cantidad esperada de alertas por dia, semana y mes.
- Dejar claro que es una estimacion operativa y pedagogica, no una estadistica cerrada del sistema.

Tabla propuesta:

| Puntaje tecnico | Ganadas aprox. de 10 | Probabilidad aprox. de ganar | Alertas por dia | Alertas por semana | Alertas por mes |
|---|---:|---:|---:|---:|---:|
| `72` | `5 a 6` | `55% a 60%` | `1 a 2` | `7 a 14` | `30 a 60` |
| `74` | `6` | `60%` | `1 a 2` | `6 a 12` | `25 a 50` |
| `76` | `6 a 7` | `60% a 65%` | `1 a 2` | `5 a 10` | `20 a 40` |
| `78` | `6 a 7` solido | `65% a 70%` | `0 a 1` o `1` | `4 a 8` | `16 a 32` |
| `80` | `7` | `70%` | `0 a 1` | `3 a 6` | `12 a 24` |

Idea de implementacion:
- Mostrar esta escala en la UI o en documentacion interna como referencia rapida.
- Usarla solo como traduccion humana del `puntaje tecnico`.
- Nunca presentarla como precision historica comprobada mientras el worker no tenga suficiente muestra resuelta.


## alerta-telegram-sl-guia-doble-tp

Fecha: 2026-03-25

Objetivo: agregar a la alerta del worker un `SL guia` y dos `TP` sin quitar informacion actual, para que la alerta sea mas operativa en Telegram.

Resumen:
- Mantener intacta la alerta actual.
- Agregar `SL guia` medido desde la `entrada guia`, pero ubicado por estructura:
  - `short`: por encima del ultimo `high` valido
  - `long`: por debajo del ultimo `low` valido
- Mostrar el riesgo del `SL` como porcentaje real desde la entrada.
- Agregar dos objetivos:
  - `TP conservador (2R)`
  - `TP estimado (RR estimado actual)`
- El `TP` debe calcularse desde la entrada usando el porcentaje de riesgo del `SL`, no como un numero visual arbitrario.
- Mantener un solo bloque Telegram, sin quitar `Mentor`, `Puntaje tecnico`, `Checklist tecnico`, `Vela UTC` ni `Patron`.

Regla de calculo:
- `SL % = distancia entre entrada y SL / entrada`
- `TP conservador = 2R`
- `TP estimado = RR estimado actual`
- Para `short`:
  - `SL precio` va arriba de la entrada
  - `TP` va abajo de la entrada
- Para `long`:
  - `SL precio` va abajo de la entrada
  - `TP` va arriba de la entrada

Formato propuesto:

```text
[Estrella Trader] BNB-USD | 1D + 4H

Direccion: BAJISTA
Accion: Esperar confirmacion de venta
Escenario: Pullback en tendencia
Entrada guia: 645.1921386719
SL guia: 649.98 | Riesgo: +0.74%
TP conservador (2R): 635.62 | Objetivo: -1.48%
TP estimado (2.89R): 631.36 | Objetivo: -2.14%
Fuerza: DEBIL
Riesgo/beneficio: 1/2.89
Puntaje tecnico: 77/100
Checklist tecnico: 4/4
Vela UTC: 2026-03-25T12:00:00Z
Patron: sin_patron

Mentor:
...
```

Idea de implementacion:
- Calcular `SL guia` desde estructura valida reciente.
- Exponer en el estado del worker:
  - `sl_price`
  - `sl_pct`
  - `tp_conservative_price`
  - `tp_conservative_pct`
  - `tp_estimated_price`
  - `tp_estimated_pct`
- Insertar estas lineas justo despues de `Entrada guia`.
- Mantener Telegram con una sola salida compacta, y dejar mas detalle para la web si hiciera falta.

## microscalping-para-luego

Fecha: 2026-03-28

Objetivo: evaluar mas adelante una excepcion operativa para `MS` sin contaminar la alerta principal ni mezclarla con el modelo actual.

Resumen:
- Mantener el marco actual como base:
  - `SL` normal entre `0.5%` y `1.0%`
  - `TP` normal entre `1.0%` y `2.0%`
  - relacion objetivo `1:2`
- Si se retoma `MS`, permitir un `SL` mas corto que `0.5%`, pero solo dentro de un marco propio de microscalping.
- No mezclar esta logica en la alerta visible por ahora, para no alargar ni complicar el mensaje.

Nota:
- La prioridad sigue siendo respetar el modelo que ya ha funcionado.
- Si una idea necesita un `SL` demasiado amplio para ese marco, no encaja con el estilo actual.
- Si se estudia `MS` despues, debe definirse con limites propios y validacion separada.
