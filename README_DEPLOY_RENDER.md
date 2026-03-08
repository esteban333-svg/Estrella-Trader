# Deploy En Render (Reproducible)

Este repo incluye `render.yaml` para crear dos servicios:

- `estrella-trader-web`: Streamlit app.
- `estrella-trader-scanner`: worker de alertas con estado persistente en disco.

## 1) Deploy inicial

1. Sube el repo a GitHub.
2. En Render: `New +` -> `Blueprint`.
3. Conecta el repo y selecciona el branch de produccion.
4. Render detecta `render.yaml` y crea ambos servicios.

## 2) Variables de entorno

Variables secretas recomendadas:

- `AUTH_COOKIE_PASSWORD`
- `PREMIUM_ACCESS_CODE` (si quieres habilitar activacion por codigo)
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_BOT_USERNAME`
- `ALERT_EMAIL_TO`
- `ALERT_TELEGRAM_CHAT_IDS`
- `RELEASE_VERSION` (ej: `2026.03.08-r1`)

Referencia rapida: `env.render.example`.

Antes de desplegar, corre validacion local:

```powershell
.\safe_update_check.ps1
```

Variables operativas del scanner:

- `SCANNER_LOG_MAX_MB` (default 20)
- `SCANNER_LOG_BACKUP_COUNT` (default 5)
- `SCANNER_RESOURCE_PROFILE` (`render_512mb` recomendado para plan 512MB)

## 3) Salud y operacion

Worker escribe:

- `/var/data/scanner.log`
- `/var/data/scanner_state.json`
- `/var/data/scanner_health.json`

Chequeo local:

```powershell
.\.venv\Scripts\python.exe check_scanner_health.py --health scanner_health.json --max-stale-sec 240
```

## 4) Rollback rapido

Ruta recomendada:

1. Abre servicio en Render.
2. Ve a `Deploys`.
3. Selecciona el ultimo deploy estable.
4. Click en `Rollback` (o `Redeploy` del deploy estable).

Practica recomendada para rollback mas rapido:

1. Antes de deploy, asigna `RELEASE_VERSION` (ej: `2026.03.08-r1`).
2. Crea tag estable:
   - `.\mark_release_tag.ps1 -Prefix stable -Push`
3. Si falla, prepara branch de rollback desde ese tag:
   - `.\prepare_rollback_branch.ps1 -Tag stable-...`
4. En Render, `Manual Deploy` del branch `rollback/...` o rollback desde `Deploys`.
