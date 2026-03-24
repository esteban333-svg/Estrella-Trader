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
