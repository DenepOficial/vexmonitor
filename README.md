# VEXEN Monitor - Railway

Esta versión usa únicamente:

```env
DATABASE_URL
```

No utiliza `DATABASE_PUBLIC_URL`.

## Variables en Railway

```env
DISCORD_TOKEN=...
DATABASE_URL=${{Postgres.DATABASE_URL}}
DB_SCHEMA=vex_monitor
ROUTE_REFRESH_SECONDS=20
LOG_LEVEL=INFO
```

## Archivos

- `main.py`
- `database.py`
- `requirements.txt`
- `railway.toml`
- `.env.example`

## Tabla principal

```text
vex_monitor.monitor_routes
```

Cada fila define:

```text
Servidor origen / Canal origen
             ↓
Servidor destino / Canal destino
```

El bot no crea canales.
