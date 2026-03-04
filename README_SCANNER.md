# Scanner Dorado (sin Streamlit)

Este scanner corre como proceso independiente y sigue enviando alertas aunque cierres la web/app de Streamlit.
Canales soportados:

- Email (Gmail/SMTP)
- Telegram (movil + desktop)
- Notificacion de Windows (toast local en ese PC)

## Archivos

- `scanner_worker.py`: loop de escaneo forex + cripto y envio de alertas.
- `scanner_config.json`: configuracion de watchlist, intervalo y destino de alertas.
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
- `poll_interval_sec`: cada cuantos segundos escanea
- `interval`: temporalidad de velas (`1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d`)
- `scan_forex` / `scan_crypto`
- `forex_pairs` / `crypto_symbols`
- `cooldown_minutes`: evita spam de alertas repetidas
- `notification.*.enabled`: activa/desactiva cada canal

## 3) Probar un ciclo

```powershell
.\start_scanner.ps1 -Once -Debug
```

Revisa logs en `scanner.log` y estado en `scanner_state.json`.

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
