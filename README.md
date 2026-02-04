# GMC Veraset API Proxy

Serverless API en Vercel para conectar con Veraset.

## Setup Rápido

```bash
# 1. Instalar Vercel CLI
npm i -g vercel

# 2. Login
vercel login

# 3. Configurar variables de entorno
vercel env add VERASET_API_KEY

# 4. Deploy
vercel --prod
```

## Endpoints Disponibles

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/api/health` | GET | Health check |
| `/api/veraset/pois` | POST | Buscar POIs |
| `/api/veraset/visits` | POST | Obtener visitas |
| `/api/veraset/movement` | POST | Movement data |
| `/api/veraset/job/[id]` | GET | Check job status |

## Variables de Entorno

```
VERASET_API_KEY=tu_api_key_aqui
```
