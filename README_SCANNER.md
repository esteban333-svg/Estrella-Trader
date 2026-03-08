# Scanner Dorado (sin Streamlit)

Este scanner corre como proceso independiente y sigue enviando alertas aunque cierres la web/app de Streamlit.
Canales soportados:

- Email (Gmail/SMTP)
- Telegram (movil + desktop)
- Notificacion de Windows (toast local en ese PC)

## Archivos

- `scanner_worker.py`: loop de escaneo forex + cripto y envio de alertas.
- `scanner_config.json`: configuracion de watchlist, intervalo y destino de alertas.
- `scanner_health.json`: estado de salud operativo (errores, latencia, envios).
- `check_scanner_health.py`: valida salud y retorna exit code util para monitoreo.
- `safe_update_check.ps1`: compila + ejecuta tests antes de deploy.
- `start_scanner.ps1`: inicia el worker en segundo plano.
- `stop_scanner.ps1`: detiene el worker.
- `install_scanner_task.ps1`: crea tarea de Windows para autoarranque al iniciar sesion.
- `uninstall_scanner_task.ps1`: elimina esa tarea.

## 1) Configurar canales

### Email (opcional)

Variables de entorno:

- `SMTP_HOST` (ej: `smtp.gmail.com`)
- `SMTP_PORT` (ej: `587`)
- `SMTP_USER`
- `SMTP_PASSWORD`
- `SMTP_FROM`

Destinatarios de email:

- En `scanner_config.json` -> `notification.email.to` (lista)
- (legacy) `notification.email_to` (lista)
- o variable `ALERT_EMAIL_TO` con correos separados por coma.

### Telegram (principal por cuenta)

Variables y config:

- `TELEGRAM_BOT_TOKEN` en variable de entorno de Windows/usuario.
- `TELEGRAM_BOT_USERNAME` (opcional, para el boton "Abrir bot de Telegram" en la UI).
- Cada usuario debe guardar su `Chat ID Telegram` en la seccion `Cuenta` de la app.
- `scanner_config.json` -> `notification.telegram.chat_ids`
  - Opcional como fallback/admin si no hay usuarios con chat id configurado.
  - Recomendado: dejarlo vacio en repo y usar `ALERT_TELEGRAM_CHAT_IDS` en entorno.
- `scanner_config.json` -> `notification.telegram.send_coin_image` (`true/false`, default `true`).
- `scanner_config.json` -> `notification.telegram.coin_image_urls` (dict opcional por simbolo, ej: `"BTC": "https://..."`).
- Opcional: `ALERT_TELEGRAM_CHAT_IDS` (ids separados por coma).

### Limites por plan (automatico)

- Usuario gratis: `1` señal Dorado por dia por mercado (ejemplo: `Forex|EUR/USD`, `Cripto|BTC`).
- Usuario premium: sin limite (recibe todas las activaciones Dorado).
- El limite se aplica en `scanner_worker.py` aunque la web/app este cerrada.

### Windows toast (opcional)

- `scanner_config.json` -> `notification.windows.enabled`
- Solo aplica en Windows y en la sesion de usuario donde corre el worker.

## 2) Ajustar watchlist e intervalo

Edita `scanner_config.json`:

- `analysis_mode`: `tendencial` o `estructural`
  - `tendencial`: usa `period` + `interval` de config.
  - `estructural`: usa 1D + 4H y en alerta mostrara `Modo: Estructural (1D+4H)`.
- `auto_multi_interval`:
  - `true`: escanea automaticamente multiples temporalidades (por defecto `15m`, `30m`, `1h`, `4h`) sin cambiar `interval` manualmente.
  - `false`: vuelve al modo clasico de una sola temporalidad (`interval`) + opcional estructural.
- `scan_intervals`: lista de temporalidades a escanear cuando `auto_multi_interval=true`.
- `scan_structural_1d_4h`: si `true`, agrega tambien alertas estructurales `1D + 4H`.
- `poll_interval_sec`: cada cuantos segundos escanea
- `interval`: temporalidad de velas (`1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d`)
- `scan_forex` / `scan_crypto` / `scan_gold`
- `forex_pairs` / `crypto_symbols` / `gold_symbols`
  - Oro default: `XAU/USD` (`GC=F` en yfinance, `XAU/USD` en TwelveData)
- `cooldown_minutes`: evita spam de alertas repetidas
- `notification.*.enabled`: activa/desactiva cada canal
- `scanner_worker.py` rota logs automaticamente (`SCANNER_LOG_MAX_MB`, `SCANNER_LOG_BACKUP_COUNT`).
- Perfil de recursos: `SCANNER_RESOURCE_PROFILE=render_512mb` reduce carga (simbolos/timeframes) para 512MB.
- Errores de canales se guardan redactados (token/chat/email ocultos).
- `precision_filters.*` (modo precision alta):
  - `alert_profile`: `conservador` / `balanceado` / `agresivo` (ajuste rapido de exigencia).
  - `require_closed_candle`: solo analiza velas cerradas (si hay vela en formacion, la descarta).
  - `persistence_bars`: exige n velas consecutivas con senal valida antes de alertar.
  - `multi_timeframe_filter`: valida alineacion multi-timeframe (15m/30m -> 1h/4h/1d).
  - `require_price_action_confirmation`: exige patron de vela a favor (envolvente/rechazo).
  - `min_confidence_score` y `min_rr`: umbrales minimos para alertar.
  - `adaptive_threshold` y `adaptive_cooldown`: ajustan exigencia/cooldown por volatilidad.
  - `quality_calibration_enabled`: calibra umbrales con datos reales de `quality_stats` para reducir ruido.
  - `quality_calibration_min_resolved`: cantidad minima de alertas resueltas para aplicar calibracion.
  - `quality_calibration_scope`: `global`, `record` o `global_and_record` (recomendado).
  - `quality_calibration_record_enabled`: activa ajuste por simbolo/timeframe.
  - `quality_calibration_record_min_resolved`: minimo de historico por simbolo/timeframe para calibrar.
  - `max_alerts_per_symbol_day`: tope diario por simbolo para evitar sobre-alerta.
  - `quality_window_bars` / `quality_window_bars_by_interval`: ventana para medir acierto de alerta
    con regla `+1R antes de -1R`.

### Seguimiento de precision por alerta

- El scanner guarda una alerta "abierta" por simbolo (`open_alert`) con TP/SL a 1R (ATR14).
- Resultado posible: `win`, `loss`, `timeout` o `replaced`.
- Se almacena historial en `quality_history` y estadisticas en `quality_stats` dentro de `scanner_state.json`.
- Se guardan tambien `effective_thresholds` y `quality_calibration` por simbolo/timeframe.
- La UI Premium (debug) muestra panel de precision, ranking de activos operables y RR promedio.

## 3) Probar un ciclo

```powershell
.\start_scanner.ps1 -Once -Debug
```

Revisa logs en `scanner.log` y estado en `scanner_state.json`.
Salud operativa en `scanner_health.json`.

Chequeo rapido de salud:

```powershell
.\.venv\Scripts\python.exe check_scanner_health.py --health scanner_health.json --max-stale-sec 240
```

Validacion segura antes de actualizar:

```powershell
.\safe_update_check.ps1
```

## 4) Dejarlo corriendo en segundo plano

```powershell
.\start_scanner.ps1
```

Para detenerlo:

```powershell
.\stop_scanner.ps1
```

## 5) Autoarranque al iniciar Windows

```powershell
.\install_scanner_task.ps1
```

Eliminar tarea:

```powershell
.\uninstall_scanner_task.ps1
```

Con esto, el scanner seguira activo aunque cierres Streamlit.
