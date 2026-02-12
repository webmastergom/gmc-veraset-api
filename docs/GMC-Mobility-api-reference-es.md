# API de Movilidad - Referencia de API Externa

## Autenticaci&oacute;n

Todas las peticiones requieren una API key. Incl&uacute;yela en el header `X-API-Key`:

```bash
curl -H "X-API-Key: TU_API_KEY" https://tu-dominio.com/api/external/jobs
```

Alternativamente, puedes pasarla como par&aacute;metro de query: `?api_key=TU_API_KEY`

## L&iacute;mites de Tasa

- **100 peticiones por minuto** por API key
- Los headers de rate limit se incluyen en cada respuesta:
  - `X-RateLimit-Limit`: M&aacute;ximo de peticiones por ventana
  - `X-RateLimit-Remaining`: Peticiones restantes
  - `X-RateLimit-Reset`: Timestamp Unix cuando se resetea el l&iacute;mite

---

## Endpoints

### 1. Crear Job

**`POST /api/external/jobs`**

Env&iacute;a un job de an&aacute;lisis de movilidad proporcionando POIs, rango de fechas y configuraci&oacute;n.

#### Cuerpo de la Petici&oacute;n

| Campo | Tipo | Requerido | Descripci&oacute;n |
|-------|------|-----------|-------------|
| `name` | string | S&iacute; | Nombre del job (1-200 caracteres) |
| `country` | string | S&iacute; | C&oacute;digo de pa&iacute;s ISO 2 letras (ej: "ES") |
| `type` | string | No | Tipo de job: `pings`, `devices` o `aggregate`. Default: `pings` |
| `date_range.from` | string | S&iacute; | Fecha inicio (YYYY-MM-DD) |
| `date_range.to` | string | S&iacute; | Fecha fin (YYYY-MM-DD). M&aacute;ximo 31 d&iacute;as desde inicio. |
| `radius` | number | No | Radio de b&uacute;squeda en metros (1-1000). Default: 10 |
| `schema` | string | No | Schema de datos: `BASIC`, `ENHANCED` o `FULL`. Default: `BASIC` |
| `pois` | array | S&iacute; | Array de objetos POI (1-25,000 elementos) |
| `pois[].id` | string | S&iacute; | Identificador &uacute;nico del POI |
| `pois[].name` | string | No | Nombre legible del POI |
| `pois[].latitude` | number | S&iacute; | Latitud (-90 a 90) |
| `pois[].longitude` | number | S&iacute; | Longitud (-180 a 180) |
| `webhook_url` | string | No | URL HTTPS para notificaciones de cambio de estado |

#### Ejemplo de Petici&oacute;n

```bash
curl -X POST https://tu-dominio.com/api/external/jobs \
  -H "X-API-Key: TU_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Estancos Espana - Enero 2026",
    "country": "ES",
    "type": "pings",
    "date_range": { "from": "2026-01-01", "to": "2026-01-31" },
    "radius": 10,
    "schema": "BASIC",
    "pois": [
      { "id": "estanco_001", "name": "Estanco Sol", "latitude": 40.4168, "longitude": -3.7038 },
      { "id": "estanco_002", "name": "Estanco Gran Via", "latitude": 40.4200, "longitude": -3.7050 }
    ],
    "webhook_url": "https://tu-servidor.com/webhooks/movilidad"
  }'
```

#### Respuesta (201 Created)

```json
{
  "job_id": "abc123xyz",
  "status": "QUEUED",
  "poi_count": 2,
  "date_range": { "from": "2026-01-01", "to": "2026-01-31" },
  "created_at": "2026-02-07T10:00:00.000Z",
  "status_url": "/api/external/jobs/abc123xyz/status",
  "webhook_registered": true
}
```

---

### 2. Consultar Estado del Job

**`GET /api/external/jobs/:jobId/status`**

Consulta el estado actual de un job. Si el job a&uacute;n se est&aacute; procesando, este endpoint verifica autom&aacute;ticamente el estado con el proveedor upstream.

#### Ejemplo de Petici&oacute;n

```bash
curl -H "X-API-Key: TU_API_KEY" \
  https://tu-dominio.com/api/external/jobs/abc123xyz/status
```

#### Respuesta - En Proceso

```json
{
  "job_id": "abc123xyz",
  "status": "RUNNING",
  "created_at": "2026-02-07T10:00:00.000Z",
  "updated_at": "2026-02-07T10:05:00.000Z"
}
```

#### Respuesta - Completado

```json
{
  "job_id": "abc123xyz",
  "status": "SUCCESS",
  "created_at": "2026-02-07T10:00:00.000Z",
  "completed_at": "2026-02-07T12:30:00.000Z",
  "results": {
    "total_pings": 1234567,
    "total_devices": 45678,
    "date_range": { "from": "2026-01-01", "to": "2026-01-31" },
    "poi_count": 2,
    "poi_summary": [
      { "poi_id": "estanco_001", "name": "Estanco Sol", "pings": 5432, "devices": 234 },
      { "poi_id": "estanco_002", "name": "Estanco Gran Via", "pings": 3210, "devices": 189 }
    ],
    "catchment": {
      "available": true,
      "url": "/api/external/jobs/abc123xyz/catchment"
    }
  }
}
```

#### Respuesta - Fallido

```json
{
  "job_id": "abc123xyz",
  "status": "FAILED",
  "created_at": "2026-02-07T10:00:00.000Z",
  "error": "Descripcion del error de procesamiento"
}
```

#### Estados del Job

| Estado | Descripci&oacute;n |
|--------|-------------|
| `QUEUED` | Job enviado, esperando ser procesado |
| `RUNNING` | Job en proceso |
| `SUCCESS` | Job completado exitosamente, resultados disponibles |
| `FAILED` | Job fall&oacute;, revisar campo `error` |

---

### 3. Obtener An&aacute;lisis de Catchment

**`GET /api/external/jobs/:jobId/catchment`**

Obtiene la distribuci&oacute;n de c&oacute;digos postales residenciales de los visitantes que pasaron por los POIs. Este an&aacute;lisis determina d&oacute;nde viven los visitantes basado en patrones de ubicaci&oacute;n nocturna.

Solo disponible para jobs con estado `SUCCESS`.

#### Ejemplo de Petici&oacute;n

```bash
curl -H "X-API-Key: TU_API_KEY" \
  https://tu-dominio.com/api/external/jobs/abc123xyz/catchment
```

#### Respuesta

```json
{
  "job_id": "abc123xyz",
  "analyzed_at": "2026-02-07T13:00:00.000Z",
  "summary": {
    "total_devices_analyzed": 45678,
    "devices_with_home_location": 32100,
    "devices_matched_to_zipcode": 28500,
    "total_zipcodes": 156,
    "top_zipcode": "28001",
    "top_city": "Madrid"
  },
  "zipcodes": [
    {
      "zipcode": "28001",
      "city": "Madrid",
      "province": "Madrid",
      "region": "Comunidad de Madrid",
      "devices": 1234,
      "percentage": 4.33
    },
    {
      "zipcode": "08001",
      "city": "Barcelona",
      "province": "Barcelona",
      "region": "Cataluna",
      "devices": 987,
      "percentage": 3.46
    }
  ]
}
```

> **Nota:** Por privacidad, solo se incluyen c&oacute;digos postales con 5 o m&aacute;s dispositivos.

> **Nota:** La primera petici&oacute;n puede tardar 1-3 minutos ya que ejecuta consultas Athena y reverse geocoding. Los resultados se cachean para peticiones posteriores.

---

### 4. Listar Jobs Disponibles

**`GET /api/external/jobs`**

Lista todos los jobs completados con datasets disponibles.

#### Ejemplo de Petici&oacute;n

```bash
curl -H "X-API-Key: TU_API_KEY" \
  https://tu-dominio.com/api/external/jobs
```

#### Respuesta

```json
{
  "jobs": [
    {
      "datasetName": "spain-tobacco-jan-2026",
      "jobName": "Estancos Espana - Enero 2026",
      "jobId": "abc123xyz",
      "dateRange": { "from": "2026-01-01", "to": "2026-01-31" },
      "status": "SUCCESS",
      "poiCount": 150
    }
  ]
}
```

---

## Webhooks

Si proporcionas un `webhook_url` al crear un job, recibir&aacute;s notificaciones HTTP POST cuando el estado del job cambie.

### Payload del Webhook

```json
{
  "event": "job.status_changed",
  "job_id": "abc123xyz",
  "status": "SUCCESS",
  "previous_status": "RUNNING",
  "timestamp": "2026-02-07T12:30:00.000Z",
  "results_url": "/api/external/jobs/abc123xyz/status"
}
```

### Requisitos del Webhook

- La URL debe usar HTTPS
- Tu endpoint debe responder con 2xx dentro de 10 segundos
- Los webhooks fallidos se reintentan una vez despu&eacute;s de 2 segundos
- Las peticiones webhook incluyen `Content-Type: application/json`

---

## C&oacute;digos de Error

| HTTP Status | Error | Descripci&oacute;n |
|-------------|-------|-------------|
| 400 | Error de Validaci&oacute;n | El cuerpo de la petici&oacute;n fall&oacute; la validaci&oacute;n. Revisa el mensaje de error. |
| 401 | No Autorizado | API key faltante o inv&aacute;lida |
| 404 | No Encontrado | Job ID no encontrado |
| 409 | Job No Listo | Job no completado a&uacute;n (para peticiones de catchment) |
| 429 | Rate Limited | Demasiadas peticiones. Revisa header `Retry-After`. |
| 502 | Error Upstream | La llamada a la API de Veraset fall&oacute; |
| 500 | Error Interno | Error del servidor |

---

## Flujo T&iacute;pico de Uso

```
1. POST /api/external/jobs             --> Crear job, obtener job_id
2. GET  /api/external/jobs/:id/status  --> Consultar hasta que status = SUCCESS
   (o esperar notificaci&oacute;n de webhook)
3. GET  /api/external/jobs/:id/status  --> Obtener resultados (pings, dispositivos, por POI)
4. GET  /api/external/jobs/:id/catchment --> Obtener matriz de c&oacute;digos postales residenciales
```

Intervalo de polling recomendado: **cada 5 minutos** (los jobs t&iacute;picamente tardan 1-4 horas).
