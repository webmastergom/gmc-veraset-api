# API de Movilidad - Referencia de API Externa

## Quickstart: Tu primer job en 5 minutos

Este ejemplo crea un job, espera a que termine y descarga los resultados. Copia, pega y ejecuta.

### Bash (curl + jq)

```bash
# 1. Configura tu API key
API_KEY="TU_API_KEY"
BASE_URL="https://tu-dominio.com"

# 2. Crea un job
JOB_RESPONSE=$(curl -s -X POST "$BASE_URL/api/external/jobs" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Mi primer job",
    "country": "ES",
    "type": "pings",
    "date_range": { "from": "2026-01-01", "to": "2026-01-07" },
    "radius": 50,
    "pois": [
      { "id": "poi_001", "name": "Puerta del Sol", "latitude": 40.4168, "longitude": -3.7038 }
    ]
  }')

JOB_ID=$(echo "$JOB_RESPONSE" | jq -r '.job_id')
echo "Job creado: $JOB_ID"

# 3. Espera a que termine (consulta cada 5 minutos)
while true; do
  STATUS_RESPONSE=$(curl -s "$BASE_URL/api/external/jobs/$JOB_ID/status" \
    -H "X-API-Key: $API_KEY")
  STATUS=$(echo "$STATUS_RESPONSE" | jq -r '.status')
  echo "Estado: $STATUS"

  if [ "$STATUS" = "SUCCESS" ]; then
    echo "Resultados:"
    echo "$STATUS_RESPONSE" | jq '.results'
    break
  elif [ "$STATUS" = "FAILED" ]; then
    echo "Error: $(echo "$STATUS_RESPONSE" | jq -r '.error')"
    break
  fi

  sleep 300  # 5 minutos
done

# 4. Obtener catchment (origenes residenciales) — global
curl -s "$BASE_URL/api/external/jobs/$JOB_ID/catchment" \
  -H "X-API-Key: $API_KEY" | jq '.'

# 5. Obtener catchment de un solo POI
curl -s "$BASE_URL/api/external/jobs/$JOB_ID/catchment/poi_001" \
  -H "X-API-Key: $API_KEY" | jq '.'

# 6. Obtener origen-destino — global
curl -s "$BASE_URL/api/external/jobs/$JOB_ID/od" \
  -H "X-API-Key: $API_KEY" | jq '.'

# 7. Obtener origen-destino de un solo POI
curl -s "$BASE_URL/api/external/jobs/$JOB_ID/od/poi_001" \
  -H "X-API-Key: $API_KEY" | jq '.'
```

### Python

```python
import requests
import time

API_KEY = "TU_API_KEY"
BASE_URL = "https://tu-dominio.com"
HEADERS = {"X-API-Key": API_KEY, "Content-Type": "application/json"}

# 1. Crear job
job = requests.post(f"{BASE_URL}/api/external/jobs", headers=HEADERS, json={
    "name": "Mi primer job",
    "country": "ES",
    "type": "pings",
    "date_range": {"from": "2026-01-01", "to": "2026-01-07"},
    "radius": 50,
    "pois": [
        {"id": "poi_001", "name": "Puerta del Sol", "latitude": 40.4168, "longitude": -3.7038}
    ]
}).json()

job_id = job["job_id"]
print(f"Job creado: {job_id}")

# 2. Polling hasta completar
while True:
    status = requests.get(
        f"{BASE_URL}/api/external/jobs/{job_id}/status",
        headers=HEADERS
    ).json()

    print(f"Estado: {status['status']}")

    if status["status"] == "SUCCESS":
        print(f"Pings totales: {status['results']['total_pings']}")
        print(f"Dispositivos: {status['results']['total_devices']}")
        break
    elif status["status"] == "FAILED":
        print(f"Error: {status['error']}")
        break

    time.sleep(300)  # 5 minutos

# 3. Catchment global (origenes residenciales)
catchment = requests.get(
    f"{BASE_URL}/api/external/jobs/{job_id}/catchment",
    headers=HEADERS
).json()

for z in catchment["zipcodes"][:5]:
    print(f"  {z['zipcode']} {z['city']}: {z['devices']} dispositivos ({z['percentage']}%)")

# 4. Catchment de un solo POI
poi_catchment = requests.get(
    f"{BASE_URL}/api/external/jobs/{job_id}/catchment/poi_001",
    headers=HEADERS
).json()

print(f"POI: {poi_catchment['poi']['name']}, Zipcodes: {poi_catchment['summary']['total_zipcodes']}")

# 5. Origen-Destino global
od = requests.get(
    f"{BASE_URL}/api/external/jobs/{job_id}/od",
    headers=HEADERS
).json()

print(f"Top origen: {od['summary']['top_origin_city']}")
print(f"Top destino: {od['summary']['top_destination_city']}")

# 6. Origen-Destino de un solo POI
poi_od = requests.get(
    f"{BASE_URL}/api/external/jobs/{job_id}/od/poi_001",
    headers=HEADERS
).json()

print(f"POI: {poi_od['poi']['name']}, Device-days: {poi_od['summary']['total_device_days']}")
```

### JavaScript (Node.js / fetch)

```javascript
const API_KEY = "TU_API_KEY";
const BASE_URL = "https://tu-dominio.com";
const headers = { "X-API-Key": API_KEY, "Content-Type": "application/json" };

// 1. Crear job
const jobRes = await fetch(`${BASE_URL}/api/external/jobs`, {
  method: "POST",
  headers,
  body: JSON.stringify({
    name: "Mi primer job",
    country: "ES",
    type: "pings",
    date_range: { from: "2026-01-01", to: "2026-01-07" },
    radius: 50,
    pois: [
      { id: "poi_001", name: "Puerta del Sol", latitude: 40.4168, longitude: -3.7038 },
    ],
  }),
});
const job = await jobRes.json();
console.log(`Job creado: ${job.job_id}`);

// 2. Polling hasta completar
const poll = async () => {
  while (true) {
    const res = await fetch(`${BASE_URL}/api/external/jobs/${job.job_id}/status`, { headers });
    const status = await res.json();
    console.log(`Estado: ${status.status}`);

    if (status.status === "SUCCESS") return status;
    if (status.status === "FAILED") throw new Error(status.error);

    await new Promise((r) => setTimeout(r, 300_000)); // 5 minutos
  }
};

const result = await poll();
console.log(`Pings: ${result.results.total_pings}, Dispositivos: ${result.results.total_devices}`);

// 3. Catchment global
const catchment = await fetch(
  `${BASE_URL}/api/external/jobs/${job.job_id}/catchment`, { headers }
).then((r) => r.json());

// 4. Catchment de un solo POI
const poiCatchment = await fetch(
  `${BASE_URL}/api/external/jobs/${job.job_id}/catchment/poi_001`, { headers }
).then((r) => r.json());
console.log(`POI: ${poiCatchment.poi.name}, Zipcodes: ${poiCatchment.summary.total_zipcodes}`);

// 5. Origen-Destino global
const od = await fetch(
  `${BASE_URL}/api/external/jobs/${job.job_id}/od`, { headers }
).then((r) => r.json());

// 6. Origen-Destino de un solo POI
const poiOd = await fetch(
  `${BASE_URL}/api/external/jobs/${job.job_id}/od/poi_001`, { headers }
).then((r) => r.json());
console.log(`POI: ${poiOd.poi.name}, Device-days: ${poiOd.summary.total_device_days}`);
```

---

## Autenticacion

Todas las peticiones requieren una API key. Incluyela en el header `X-API-Key`:

```bash
curl -H "X-API-Key: TU_API_KEY" https://tu-dominio.com/api/external/jobs
```

Alternativamente, puedes pasarla como parametro de query: `?api_key=TU_API_KEY`

> **Nota:** El header `X-API-Key` es el metodo recomendado. El parametro de query es util para pruebas rapidas, pero expone la key en logs de servidor y historial del navegador.

---

## Limites de Tasa

- **100 peticiones por minuto** por API key (por ruta)
- Los headers de rate limit se incluyen en cada respuesta:
  - `X-RateLimit-Limit`: Maximo de peticiones por ventana
  - `X-RateLimit-Remaining`: Peticiones restantes
  - `X-RateLimit-Reset`: Timestamp Unix cuando se resetea el limite
- Al exceder el limite, recibes HTTP 429 con header `Retry-After` indicando segundos hasta el reset

### Limite Mensual de Jobs

- **200 jobs por mes** por cuenta (default, configurable por administrador)
- Al exceder el limite, recibes HTTP 429 con `{ "error": "Limit reached", "remaining": 0 }`
- El contador se resetea el primer dia de cada mes

---

## CORS

Las rutas externas (`/api/external/*`) permiten CORS desde cualquier origen. Los metodos permitidos son `GET`, `POST`, `PATCH` y `OPTIONS`. Puedes llamar la API directamente desde un navegador o aplicacion frontend.

---

## Privacidad y Proteccion de Datos

La API esta disenada para proteger la privacidad de los usuarios moviles. Todos los endpoints principales devuelven **exclusivamente datos agregados** — nunca identificadores individuales de dispositivo.

### Que se expone y que no

| Dato | Expuesto via API | Detalle |
|------|:----------------:|---------|
| Conteos agregados (pings, dispositivos por POI) | Si | Numeros totales sin identificacion individual |
| Distribucion por codigo postal (catchment, OD) | Si | Minimo 5 dispositivos por codigo postal |
| Metricas diarias (pings/dia, dispositivos/dia) | Si | Volumenes por fecha sin granularidad individual |
| MAIDs / Ad IDs (identificadores de dispositivo) | No | Nunca se incluyen en las respuestas estandar |
| Coordenadas GPS individuales | No | Solo se exponen las coordenadas de los POIs que tu enviaste |
| Trayectorias de dispositivos | No | Solo origenes/destinos agregados por codigo postal |

### Umbrales de Privacidad

- **Catchment/OD:** Solo se incluyen codigos postales con **5 o mas dispositivos**. Los codigos postales con menos dispositivos se omiten para prevenir la re-identificacion.
- **Agregaciones:** Todos los resultados son conteos (`COUNT`) y conteos distintos (`COUNT DISTINCT`). Nunca se exponen registros individuales.

### Endpoints de Exportacion de MAIDs (Acceso Restringido)

Para casos de uso especificos (activacion de audiencias, enriquecimiento de datos, integracion con DSPs), existen endpoints dedicados que permiten exportar identificadores de dispositivo (MAIDs/Ad IDs). Estos endpoints:

- **Requieren autorizacion explicita** — no estan disponibles con una API key estandar
- **Solo devuelven ad_ids anonimizados** asociados a un job o pais especifico
- **Estan sujetos a los terminos contractuales** con el proveedor de datos

Ver seccion **"Exportacion de MAIDs por Codigo Postal"** mas abajo para detalles completos de los endpoints.

> **Importante:** Si necesitas acceso a MAIDs, contacta al administrador para activar los permisos de exportacion en tu API key.

---

## Endpoints

### 1. Crear Job

**`POST /api/external/jobs`**

Envia un job de analisis de movilidad proporcionando POIs, rango de fechas y configuracion. El job se procesa de forma asincrona y los resultados estaran disponibles cuando el estado sea `SUCCESS`.

#### Cuerpo de la Peticion

| Campo | Tipo | Requerido | Descripcion |
|-------|------|-----------|-------------|
| `name` | string | Si | Nombre del job (1-200 caracteres) |
| `country` | string | Si | Codigo de pais ISO 2 letras (ej: `"ES"`, `"MX"`, `"US"`). Se convierte a mayusculas automaticamente. |
| `type` | string | No | Tipo de job (ver tabla abajo). Default: `"pings"` |
| `schema` | string | No | Nivel de detalle del dataset: `"BASIC"`, `"ENHANCED"` o `"FULL"`. Default: `"BASIC"` |
| `date_range.from` | string | Si | Fecha inicio `YYYY-MM-DD` |
| `date_range.to` | string | Si | Fecha fin `YYYY-MM-DD`. Maximo 31 dias desde inicio (ambas fechas inclusivas). |
| `radius` | number | No | Radio de busqueda en metros alrededor de cada POI (1-1000). Default: `10` |
| `pois` | array | Si | Array de objetos POI (1-25,000 elementos) |
| `pois[].id` | string | Si | Identificador unico del POI (1-200 caracteres) |
| `pois[].name` | string | No | Nombre legible del POI (max 500 caracteres) |
| `pois[].latitude` | number | Si | Latitud (-90 a 90) |
| `pois[].longitude` | number | Si | Longitud (-180 a 180) |
| `webhook_url` | string | No | URL HTTPS para notificaciones de cambio de estado |

#### Tipos de Job

| Tipo | Descripcion |
|------|-------------|
| `pings` | Todos los pings GPS dentro del radio de cada POI. Un ping = una lectura de ubicacion de un dispositivo. Es el tipo mas comun. |
| `devices` | Lista de dispositivos unicos que visitaron cada POI (sin pings individuales). |
| `aggregate` | Conteos agregados por POI y periodo. Util para volumenes sin datos granulares. |
| `cohort` | Agrupa dispositivos en cohortes basadas en patrones de visita. |
| `pings_by_device` | Pings organizados por dispositivo en vez de por tiempo. Util para analizar trayectorias individuales. |

#### Niveles de Schema

| Schema | Descripcion |
|--------|-------------|
| `BASIC` | Campos esenciales: coordenadas, timestamps, POI IDs. Suficiente para la mayoria de analisis. |
| `ENHANCED` | Campos adicionales de metadata del dispositivo. |
| `FULL` | Todos los campos disponibles incluyendo datos de precision y contexto. |

#### Ejemplo de Peticion

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

#### Respuestas de Error

```json
// 400 - Error de validacion
{
  "error": "Validation error: name: String must contain at least 1 character(s), date_range: Date range must be between 1 and 31 days"
}

// 401 - API key invalida
{
  "error": "Unauthorized",
  "message": "Invalid API key. Please check your API key and try again."
}

// 429 - Limite de uso alcanzado
{
  "error": "Limit reached",
  "message": "Monthly job creation limit exceeded",
  "remaining": 0
}
```

---

### 2. Consultar Estado del Job

**`GET /api/external/jobs/:jobId/status`**

Consulta el estado actual de un job. Si el job aun se esta procesando, este endpoint **automaticamente verifica el estado con Veraset** y, cuando detecta que el job termino (`SUCCESS`), **sincroniza los datos automaticamente** en la misma peticion. Esto significa que no necesitas hacer nada adicional: simplemente sigue consultando este endpoint y cuando el status sea `SUCCESS`, los resultados ya estaran listos.

#### Ejemplo de Peticion

```bash
curl -H "X-API-Key: TU_API_KEY" \
  https://tu-dominio.com/api/external/jobs/abc123xyz/status
```

#### Respuesta - En Cola / En Proceso

```json
{
  "job_id": "abc123xyz",
  "name": "Estancos Espana - Enero 2026",
  "status": "QUEUED",
  "synced": false,
  "created_at": "2026-02-07T10:00:00.000Z",
  "updated_at": "2026-02-07T10:00:00.000Z",
  "pois": []
}
```

> **Nota:** El campo `pois` devuelve un array vacio mientras el job no ha completado. Los POIs con sus coordenadas se incluyen en la respuesta una vez que el status es `SUCCESS` y los datos estan sincronizados.

#### Respuesta - Completado (SUCCESS)

```json
{
  "job_id": "abc123xyz",
  "name": "Estancos Espana - Enero 2026",
  "status": "SUCCESS",
  "synced": true,
  "created_at": "2026-02-07T10:00:00.000Z",
  "completed_at": "2026-02-07T12:30:00.000Z",
  "results": {
    "date_range": { "from": "2026-01-01", "to": "2026-01-31" },
    "poi_count": 2,
    "total_pings": 1234567,
    "total_devices": 45678,
    "poi_summary": [
      { "poi_id": "estanco_001", "name": "Estanco Sol", "pings": 5432, "devices": 234 },
      { "poi_id": "estanco_002", "name": "Estanco Gran Via", "pings": 3210, "devices": 189 }
    ],
    "catchment": {
      "available": true,
      "url": "/api/external/jobs/abc123xyz/catchment"
    },
    "od_analysis": {
      "available": true,
      "url": "/api/external/jobs/abc123xyz/od"
    }
  },
  "pois": [
    { "id": "estanco_001", "name": "Estanco Sol", "latitude": 40.4168, "longitude": -3.7038 },
    { "id": "estanco_002", "name": "Estanco Gran Via", "latitude": 40.4200, "longitude": -3.7050 }
  ]
}
```

> **Nota:** Si `synced` es `false` con status `SUCCESS`, los datos aun se estan transfiriendo. El campo `results` estara parcialmente vacio (`total_pings: null`, `poi_summary: []`). Vuelve a consultar en unos minutos.

#### Respuesta - Fallido

```json
{
  "job_id": "abc123xyz",
  "name": "Estancos Espana - Enero 2026",
  "status": "FAILED",
  "synced": false,
  "created_at": "2026-02-07T10:00:00.000Z",
  "error": "Descripcion del error de procesamiento",
  "pois": []
}
```

#### Estados del Job

| Estado | Descripcion | Accion recomendada |
|--------|-------------|-------------------|
| `QUEUED` | Job enviado, esperando ser procesado | Seguir consultando cada 5 min |
| `SCHEDULED` | Job programado, pendiente de entrar en cola | Seguir consultando cada 5 min |
| `RUNNING` | Job en proceso activo | Seguir consultando cada 5 min |
| `SUCCESS` | Job completado, resultados disponibles | Leer `results`, pedir catchment/OD |
| `FAILED` | Job fallo, revisar campo `error` | Diagnosticar y re-enviar si es necesario |

---

### 3. Verificar Estado de Multiples Jobs (Batch Poll)

**`POST /api/external/jobs/poll`**

Verifica el estado de **todos los jobs pendientes** (`QUEUED`, `RUNNING`, `SCHEDULED`) en una sola peticion. Util cuando tienes multiples jobs en proceso y quieres verificar todos a la vez sin hacer polling individual.

Este endpoint consulta la API de Veraset para cada job pendiente, actualiza los estados, y sincroniza automaticamente los datos de los jobs que hayan completado.

#### Ejemplo de Peticion

```bash
curl -X POST https://tu-dominio.com/api/external/jobs/poll \
  -H "X-API-Key: TU_API_KEY"
```

#### Respuesta (200 OK)

```json
{
  "updated": [
    {
      "job_id": "abc123xyz",
      "name": "Estancos Espana - Enero 2026",
      "old_status": "RUNNING",
      "new_status": "SUCCESS",
      "synced": true
    }
  ],
  "pending": 3
}
```

| Campo | Descripcion |
|-------|-------------|
| `updated[]` | Jobs cuyo estado cambio en esta verificacion |
| `updated[].old_status` | Estado anterior del job |
| `updated[].new_status` | Nuevo estado detectado |
| `updated[].synced` | Si los datos ya se sincronizaron (solo relevante para `SUCCESS`) |
| `pending` | Numero de jobs que siguen en proceso |

> **Nota:** Este endpoint puede tardar hasta 5 minutos (maxDuration: 300s) ya que verifica cada job secuencialmente contra Veraset. Los webhooks configurados se disparan automaticamente para cada job que cambie de estado.

---

### 4. Obtener Analisis de Catchment (Origenes Residenciales)

**`GET /api/external/jobs/:jobId/catchment`**
**`GET /api/external/jobs/:jobId/catchment/:poiId`**

Determina **donde viven los visitantes** de tus POIs. Para cada dispositivo que visito un POI, se toma el primer ping GPS de cada dia (que representa su ubicacion de origen/residencia) y se hace reverse geocoding al codigo postal mas cercano.

Solo disponible para jobs con estado `SUCCESS` y datos sincronizados.

> **Importante:** La primera peticion ejecuta consultas Athena y reverse geocoding, lo cual puede tardar **1-5 minutos**. Los resultados se cachean automaticamente — las peticiones posteriores son instantaneas. El header `X-Cache` indica `HIT` (cacheado) o `MISS` (calculado en tiempo real).

#### Filtro por POI

Por defecto el catchment incluye visitantes de **todos los POIs** del job. Puedes filtrar de dos formas:

| Forma | Ejemplo | Descripcion |
|-------|---------|-------------|
| Query param | `?poi_ids=estanco_001,estanco_002` | Multiples POIs a la vez |
| Path param | `/catchment/estanco_001` | Un solo POI |

Si un POI ID no existe en el job, recibes un error 400 con la lista de `available_poi_ids`.

> **Nota tecnica (matching de POI IDs):** Internamente, Veraset asigna IDs propios a los POIs (formato `geo_radius_X`). Los datos Parquet pueden contener el ID original del cliente (ej: `estanco_001`) o el ID de Veraset (ej: `geo_radius_0`). La API busca automaticamente con **ambos formatos** para garantizar resultados correctos independientemente de cual use el dataset.

#### Ejemplos de Peticion

```bash
# Catchment de todos los POIs (comportamiento por defecto)
curl -H "X-API-Key: TU_API_KEY" \
  https://tu-dominio.com/api/external/jobs/abc123xyz/catchment

# Catchment filtrado a POIs especificos
curl -H "X-API-Key: TU_API_KEY" \
  "https://tu-dominio.com/api/external/jobs/abc123xyz/catchment?poi_ids=estanco_001,estanco_002"

# Catchment de un solo POI
curl -H "X-API-Key: TU_API_KEY" \
  https://tu-dominio.com/api/external/jobs/abc123xyz/catchment/estanco_001
```

#### Respuesta

```json
{
  "job_id": "abc123xyz",
  "analyzed_at": "2026-02-07T13:00:00.000Z",
  "methodology": {
    "approach": "origin_first_ping",
    "description": "First GPS ping of each device-day, reverse geocoded to postal code."
  },
  "coverage": {
    "totalDevicesVisitedPois": 45678,
    "totalDeviceDays": 123456,
    "devicesMatchedToZipcode": 28500,
    "coverageRatePercent": 62.4,
    "geocodingComplete": true
  },
  "summary": {
    "total_devices_analyzed": 45678,
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
      "percentage": 4.33,
      "percentOfTotal": 4.33,
      "source": "geojson"
    },
    {
      "zipcode": "08001",
      "city": "Barcelona",
      "province": "Barcelona",
      "region": "Cataluna",
      "devices": 987,
      "percentage": 3.46,
      "percentOfTotal": 3.46,
      "source": "geojson"
    }
  ]
}
```

#### Respuesta Per-POI (`/catchment/:poiId`)

Cuando se filtra por un solo POI, la respuesta incluye un campo `poi` adicional con la identidad del POI:

```json
{
  "job_id": "abc123xyz",
  "poi": { "id": "estanco_001", "name": "Estanco Sol" },
  "analyzed_at": "2026-02-07T13:05:00.000Z",
  "methodology": { "approach": "origin_first_ping", "description": "..." },
  "coverage": {
    "totalDevicesVisitedPoi": 234,
    "totalDeviceDays": 890,
    "devicesMatchedToZipcode": 185,
    "coverageRatePercent": 79.1,
    "geocodingComplete": true
  },
  "summary": {
    "total_devices_analyzed": 234,
    "devices_matched_to_zipcode": 185,
    "total_zipcodes": 42,
    "top_zipcode": "28001",
    "top_city": "Madrid"
  },
  "zipcodes": [...]
}
```

> **Nota:** En la respuesta per-POI, el campo de coverage usa `totalDevicesVisitedPoi` (singular) en vez de `totalDevicesVisitedPois` (plural).

#### Campos Adicionales (cuando se filtra por POI)

| Campo | Presente cuando | Descripcion |
|-------|-----------------|-------------|
| `filtered_pois` | `?poi_ids=...` | Array con los POI IDs filtrados |
| `poi` | `/catchment/:poiId` | Objeto `{ id, name }` del POI individual |

#### Campos de la Respuesta

| Campo | Descripcion |
|-------|-------------|
| `methodology.approach` | Metodo utilizado: `origin_first_ping` |
| `methodology.description` | Explicacion legible del metodo |
| `coverage.totalDevicesVisitedPois` | Total de dispositivos unicos que visitaron algun POI |
| `coverage.totalDeviceDays` | Total de pares dispositivo-dia analizados |
| `coverage.devicesMatchedToZipcode` | Dispositivos que pudieron ser asignados a un codigo postal |
| `coverage.coverageRatePercent` | Porcentaje de dispositivos con match exitoso |
| `coverage.geocodingComplete` | `true` si el reverse geocoding termino para todos los registros |
| `zipcodes[].zipcode` | Codigo postal |
| `zipcodes[].city` | Ciudad |
| `zipcodes[].province` | Provincia |
| `zipcodes[].region` | Comunidad autonoma / region |
| `zipcodes[].devices` | Numero de dispositivos residentes en este codigo postal |
| `zipcodes[].percentage` | Porcentaje sobre el total de dispositivos con match |
| `zipcodes[].percentOfTotal` | Igual que `percentage` (alias para compatibilidad) |
| `zipcodes[].source` | Fuente del geocoding (ej: `"geojson"`) |

> **Privacidad:** Solo se incluyen codigos postales con 5 o mas dispositivos para proteger la privacidad individual.

---

### 5. Obtener Analisis Origen-Destino

**`GET /api/external/jobs/:jobId/od`**
**`GET /api/external/jobs/:jobId/od/:poiId`**

Analisis completo de **de donde vienen y a donde van** los visitantes de tus POIs. Para cada dispositivo que visito un POI en un dia dado:
- **Origen** = primer ping GPS del dia (de donde venia el visitante)
- **Destino** = ultimo ping GPS del dia (a donde fue despues)
- **Hora de llegada al POI** = calculada por proximidad espacial a las coordenadas del POI (distancia Haversine)

Ambos se reverse geocodean a codigo postal y se agregan.

Solo disponible para jobs con estado `SUCCESS` y datos sincronizados.

> **Importante:** La primera peticion puede tardar **1-5 minutos** (consultas Athena + reverse geocoding). Los resultados se cachean automaticamente. El header `X-Cache` indica `HIT` o `MISS`.

> **Nota tecnica (precision de visita):** La hora de llegada al POI (`poi_arrival_patterns`) y la actividad por hora (`poi_activity_by_hour`) se calculan usando **proximidad espacial** a las coordenadas del POI — no se basan en metadatos internos del dataset. Esto garantiza que solo los pings fisicamente cercanos al POI se cuentan como actividad real en el punto de interes.

#### Filtro por POI

Misma mecanica que catchment:

| Forma | Ejemplo | Descripcion |
|-------|---------|-------------|
| Query param | `?poi_ids=estanco_001,estanco_002` | Multiples POIs a la vez |
| Path param | `/od/estanco_001` | Un solo POI |

> **Nota:** Aplica el mismo matching dual de POI IDs descrito en la seccion de catchment (busca tanto por ID original como por ID de Veraset).

#### Ejemplos de Peticion

```bash
# OD de todos los POIs
curl -H "X-API-Key: TU_API_KEY" \
  https://tu-dominio.com/api/external/jobs/abc123xyz/od

# OD filtrado a POIs especificos
curl -H "X-API-Key: TU_API_KEY" \
  "https://tu-dominio.com/api/external/jobs/abc123xyz/od?poi_ids=estanco_001"

# OD de un solo POI
curl -H "X-API-Key: TU_API_KEY" \
  https://tu-dominio.com/api/external/jobs/abc123xyz/od/estanco_001
```

#### Respuesta

```json
{
  "job_id": "abc123xyz",
  "analyzed_at": "2026-02-07T14:00:00.000Z",
  "methodology": {
    "approach": "first_last_ping",
    "description": "First GPS ping = origin, last GPS ping = destination, per device-day."
  },
  "coverage": {
    "totalDeviceDays": 123456,
    "originMatchRate": 85.2,
    "destinationMatchRate": 83.7
  },
  "summary": {
    "total_device_days": 123456,
    "top_origin_zipcode": "28001",
    "top_origin_city": "Madrid",
    "top_destination_zipcode": "28006",
    "top_destination_city": "Madrid"
  },
  "origins": [
    {
      "zipcode": "28001",
      "city": "Madrid",
      "province": "Madrid",
      "region": "Comunidad de Madrid",
      "devices": 1234,
      "percentOfTotal": 4.33
    }
  ],
  "destinations": [
    {
      "zipcode": "28006",
      "city": "Madrid",
      "province": "Madrid",
      "region": "Comunidad de Madrid",
      "devices": 1100,
      "percentOfTotal": 3.87
    }
  ],
  "temporal_patterns": [
    { "hour": 0, "deviceDays": 120, "percentOfTotal": 1.2 },
    { "hour": 1, "deviceDays": 85, "percentOfTotal": 0.9 },
    { "hour": 8, "deviceDays": 2500, "percentOfTotal": 8.1 },
    { "hour": 9, "deviceDays": 3200, "percentOfTotal": 10.4 }
  ],
  "poi_arrival_patterns": [
    { "hour": 9, "deviceDays": 1800, "percentOfTotal": 14.6 },
    { "hour": 10, "deviceDays": 2100, "percentOfTotal": 17.0 },
    { "hour": 13, "deviceDays": 1500, "percentOfTotal": 12.2 }
  ],
  "poi_activity_by_hour": [
    { "hour": 9, "deviceDays": 450, "percentOfTotal": 8.3 },
    { "hour": 10, "deviceDays": 680, "percentOfTotal": 12.5 },
    { "hour": 13, "deviceDays": 520, "percentOfTotal": 9.6 }
  ]
}
```

> **Nota:** `temporal_patterns` contiene una entrada para cada hora del dia (0-23) en la que se observo actividad. Horas sin actividad pueden omitirse del array.

#### Respuesta Per-POI (`/od/:poiId`)

Cuando se filtra por un solo POI, la respuesta incluye un campo `poi` adicional:

```json
{
  "job_id": "abc123xyz",
  "poi": { "id": "estanco_001", "name": "Estanco Sol" },
  "analyzed_at": "2026-02-07T14:05:00.000Z",
  "methodology": { "approach": "first_last_ping", "description": "..." },
  "coverage": { "totalDeviceDays": 890, "originMatchRate": 82.3, "destinationMatchRate": 79.8 },
  "summary": {
    "total_device_days": 890,
    "top_origin_zipcode": "28013",
    "top_origin_city": "Madrid",
    "top_destination_zipcode": "28020",
    "top_destination_city": "Madrid"
  },
  "origins": [...],
  "destinations": [...],
  "temporal_patterns": [...],
  "poi_arrival_patterns": [...],
  "poi_activity_by_hour": [...]
}
```

#### Campos Adicionales (cuando se filtra por POI)

| Campo | Presente cuando | Descripcion |
|-------|-----------------|-------------|
| `filtered_pois` | `?poi_ids=...` | Array con los POI IDs filtrados |
| `poi` | `/od/:poiId` | Objeto `{ id, name }` del POI individual |

#### Campos de la Respuesta

| Campo | Descripcion |
|-------|-------------|
| `summary.total_device_days` | Total de pares dispositivo-dia analizados |
| `summary.top_origin_zipcode` | Codigo postal de origen mas frecuente |
| `summary.top_origin_city` | Ciudad de origen mas frecuente |
| `summary.top_destination_zipcode` | Codigo postal de destino mas frecuente |
| `summary.top_destination_city` | Ciudad de destino mas frecuente |
| `origins[]` | Array de origenes ordenados por frecuencia (misma estructura que catchment zipcodes) |
| `destinations[]` | Array de destinos ordenados por frecuencia |
| `temporal_patterns[]` | Distribucion de primeros pings del dia por hora UTC |
| `poi_arrival_patterns[]` | Patron de llegadas al POI por hora UTC (basado en proximidad espacial) |
| `poi_activity_by_hour[]` | Actividad total en el POI por hora UTC (todos los pings cercanos al POI) |

#### Campos de `temporal_patterns[]`, `poi_arrival_patterns[]` y `poi_activity_by_hour[]`

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| `hour` | number | Hora del dia UTC (0-23) |
| `deviceDays` | number | Numero de pares dispositivo-dia con actividad en esta hora |
| `percentOfTotal` | number | Porcentaje sobre el total de device-days |

---

### 5b. Analisis de Movilidad por POI (Categorias Antes/Despues)

**`GET /api/external/jobs/:jobId/mobility/:poiId`**

Analiza las categorias de POIs (del catalogo Overture) que los visitantes de un POI especifico visitaron **antes y despues** de su visita al POI medido. Utiliza una ventana temporal de ±2 horas.

> **Importante:** Este endpoint solo esta disponible a nivel per-POI (no global). Requiere el `poiId` original enviado al crear el job.

> **Nota tecnica (precision):** La deteccion de visitas al POI objetivo se realiza por **proximidad espacial** (distancia Haversine) a las coordenadas exactas del POI — no se basan en metadatos del dataset. El matching de categorias usa la base de datos Overture con un grid geohash de 9 celdas y un umbral de 200m.

#### Ejemplo

```bash
curl -s "https://tu-dominio.com/api/external/jobs/abc123xyz/mobility/poi_001" \
  -H "X-API-Key: TU_API_KEY" | jq '.'
```

#### Respuesta

```json
{
  "job_id": "abc123xyz",
  "poi": { "id": "poi_001", "name": "Mi Tienda" },
  "analyzed_at": "2026-03-18T10:30:00.000Z",
  "before": [
    { "category": "restaurant", "deviceDays": 45, "hits": 89 },
    { "category": "cafe", "deviceDays": 32, "hits": 51 }
  ],
  "after": [
    { "category": "shopping_mall", "deviceDays": 38, "hits": 65 },
    { "category": "parking", "deviceDays": 25, "hits": 42 }
  ],
  "categories": [
    { "category": "restaurant", "deviceDays": 78, "hits": 142 },
    { "category": "shopping_mall", "deviceDays": 55, "hits": 98 }
  ]
}
```

#### Campos de la Respuesta

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| `job_id` | string | ID del job |
| `poi` | object | `{ id, name }` del POI consultado |
| `analyzed_at` | string | Timestamp ISO del analisis |
| `before` | array | Categorias visitadas 0-2 horas **antes** de llegar al POI |
| `after` | array | Categorias visitadas 0-2 horas **despues** de salir del POI |
| `categories` | array | Todas las categorias combinadas (before + after, sumados) |

#### Campos de Cada Categoria

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| `category` | string | Categoria del POI visitado (del catalogo Overture, ej: `restaurant`, `cafe`, `parking`) |
| `deviceDays` | number | Numero de pares dispositivo-dia que visitaron esta categoria |
| `hits` | number | Numero total de pings/visitas a esta categoria |

#### Tiempos

| Escenario | Tiempo |
|-----------|--------|
| Primera peticion | 1-3 minutos |
| Peticion cacheada | < 1 segundo |

> **Cache:** Los resultados se cachean automaticamente en S3. El header `X-Cache` indica `HIT` o `MISS`.

---

### 6. Obtener Analisis del Dataset

**`GET /api/external/jobs/:datasetName/analysis`**

Obtiene metricas diarias del dataset: pings por dia, dispositivos por dia, y la lista de POIs activos con coordenadas.

> **Nota:** Este endpoint usa el `datasetName` (no el `jobId`). Puedes obtener el `datasetName` del endpoint "Listar Jobs" (seccion 7).

> **Nota sobre POI IDs:** Los POIs en este endpoint usan los IDs internos de Veraset (formato `geo_radius_X`), no los IDs originales del cliente. Usa el `poiMapping` del job o la respuesta del endpoint de status para correlacionar IDs internos con los tuyos.

#### Ejemplo de Peticion

```bash
curl -H "X-API-Key: TU_API_KEY" \
  https://tu-dominio.com/api/external/jobs/spain-tobacco-jan-2026/analysis
```

#### Respuesta

```json
{
  "datasetName": "spain-tobacco-jan-2026",
  "jobName": "Estancos Espana - Enero 2026",
  "analysis": {
    "dailyPings": [
      { "date": "2026-01-01", "pings": 45000 },
      { "date": "2026-01-02", "pings": 52000 }
    ],
    "dailyDevices": [
      { "date": "2026-01-01", "devices": 3200 },
      { "date": "2026-01-02", "devices": 3800 }
    ],
    "pois": [
      {
        "poiId": "geo_radius_0",
        "name": "Estanco Sol",
        "latitude": 40.4168,
        "longitude": -3.7038
      }
    ]
  }
}
```

---

### 7. Listar Jobs Disponibles

**`GET /api/external/jobs`**

Lista todos los jobs completados (`SUCCESS`) con datasets disponibles. Util para descubrir los `datasetName` necesarios para el endpoint de analisis.

#### Ejemplo de Peticion

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

> **Nota:** Solo aparecen jobs con status `SUCCESS` y datos sincronizados. Los jobs en proceso no se listan aqui — usa el endpoint de status con el `job_id` para monitorearlos.

---

### 8. Exportacion de MAIDs por Codigo Postal

Estos endpoints permiten exportar identificadores de dispositivo (MAIDs/Ad IDs) filtrados por codigos postales. **Requieren autorizacion explicita** — no estan disponibles con una API key estandar.

#### 8a. MAIDs por Codigo Postal (con Job)

**`POST /api/external/jobs/:jobId/postal-maid`**

Exporta MAIDs de dispositivos que visitaron los POIs del job y cuyo origen residencial coincide con los codigos postales especificados. Devuelve un desglose detallado por codigo postal en formato JSON.

##### Cuerpo de la Peticion

| Campo | Tipo | Requerido | Descripcion |
|-------|------|-----------|-------------|
| `postal_codes` | string[] | Si | Array de codigos postales (no vacio) |
| `country` | string | Si | Codigo de pais ISO 2 letras |
| `date_from` | string | No | Fecha inicio `YYYY-MM-DD` (filtra pings dentro del rango) |
| `date_to` | string | No | Fecha fin `YYYY-MM-DD` |

##### Ejemplo de Peticion

```bash
curl -X POST https://tu-dominio.com/api/external/jobs/abc123xyz/postal-maid \
  -H "X-API-Key: TU_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "postal_codes": ["28001", "28002"],
    "country": "ES",
    "date_from": "2026-01-01",
    "date_to": "2026-01-31"
  }'
```

##### Respuesta (200 OK — JSON)

```json
{
  "job_id": "abc123xyz",
  "analyzed_at": "2026-02-07T15:00:00.000Z",
  "filters": {
    "postal_codes": ["28001", "28002"],
    "country": "ES",
    "date_from": "2026-01-01",
    "date_to": "2026-01-31"
  },
  "methodology": { "approach": "...", "description": "..." },
  "coverage": {
    "totalDevicesInDataset": 50000,
    "totalDeviceDays": 200000,
    "devicesMatchedToPostalCodes": 1234,
    "matchedDeviceDays": 4500,
    "postalCodesRequested": 2,
    "postalCodesWithDevices": 2
  },
  "summary": {
    "totalMaids": 1234,
    "topPostalCode": "28001",
    "topPostalCodeDevices": 800
  },
  "postal_code_breakdown": [
    { "postalCode": "28001", "devices": 800, "deviceDays": 3000 },
    { "postalCode": "28002", "devices": 434, "deviceDays": 1500 }
  ],
  "devices": [
    { "ad_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx", "device_days": 3, "postal_codes": ["28001"] }
  ]
}
```

> **Nota:** El header `X-Cache` indica `HIT` o `MISS`. Los codigos postales se normalizan (trim, uppercase) y se ordenan para generar la cache key.

---

#### 8b. MAIDs por Codigo Postal (sin Job — por Pais)

**`POST /api/external/postal-maid`**

Exporta MAIDs de un pais completo filtrados por codigos postales, **sin necesidad de un job previo**. Usa un dataset pre-configurado por pais. La respuesta es un archivo **gzip comprimido** con un MAID por linea.

##### Cuerpo de la Peticion

| Campo | Tipo | Requerido | Descripcion |
|-------|------|-----------|-------------|
| `postal_codes` | string[] | Si | Array de codigos postales (no vacio) |
| `country` | string | Si | Codigo de pais ISO 2 letras |
| `date_from` | string | No | Fecha inicio `YYYY-MM-DD` |
| `date_to` | string | No | Fecha fin `YYYY-MM-DD` |

##### Ejemplo de Peticion

```bash
curl -X POST https://tu-dominio.com/api/external/postal-maid \
  -H "X-API-Key: TU_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "postal_codes": ["28001", "28002", "28003"],
    "country": "ES"
  }' \
  --output maids.txt.gz
```

##### Respuesta (200 OK — gzip)

La respuesta es un archivo gzip con los headers:

| Header | Descripcion |
|--------|-------------|
| `Content-Type` | `application/gzip` |
| `Content-Disposition` | `attachment; filename="postal-maid-ES-28001_28002_28003-20260207.txt.gz"` |
| `X-Total-Maids` | Numero total de MAIDs en el archivo |
| `X-Cache` | `HIT` o `MISS` |

El contenido descomprimido es texto plano con un MAID por linea:

```
xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy
zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz
```

##### Paises Disponibles

La lista de paises con datasets configurados puede consultarse intentando un pais no disponible. La respuesta 404 incluye:

```json
{
  "error": "No dataset configured for country: XX",
  "available_countries": ["ES", "MX", "US", "CO"]
}
```

---

## Webhooks

Si proporcionas un `webhook_url` al crear un job, recibiras notificaciones HTTP POST cuando el estado del job cambie. Esto te permite evitar el polling y reaccionar inmediatamente cuando un job termina.

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

### Transiciones de Estado Notificadas

```
QUEUED -> RUNNING      (el job comenzo a procesarse)
RUNNING -> SUCCESS     (resultados listos — llama a status, catchment u OD)
RUNNING -> FAILED      (el job fallo — revisa el error en status)
```

### Requisitos del Webhook

| Requisito | Detalle |
|-----------|---------|
| Protocolo | HTTPS obligatorio (URLs HTTP son rechazadas al crear el job) |
| Timeout | Tu endpoint debe responder con 2xx dentro de 10 segundos |
| Reintentos | 1 reintento automatico despues de 2 segundos si la primera llamada falla |
| Content-Type | `application/json` |
| User-Agent | `Veraset-API-Webhook/1.0` |

### Ejemplo: Servidor Webhook en Python (Flask)

```python
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/webhooks/movilidad", methods=["POST"])
def webhook():
    payload = request.json
    print(f"Job {payload['job_id']}: {payload['previous_status']} -> {payload['status']}")

    if payload["status"] == "SUCCESS":
        # Descargar resultados automaticamente
        print(f"Resultados listos: {payload['results_url']}")

    return jsonify({"ok": True}), 200
```

---

## Mega-Jobs (Rangos de Fechas Extendidos)

Los mega-jobs permiten analizar rangos de fechas superiores a 31 dias dividiendo automaticamente el trabajo en sub-jobs de 31 dias maximo. El sistema maneja la creacion, monitoreo y consolidacion de todos los sub-jobs de forma transparente.

### Quickstart: Tu primer mega-job

Este ejemplo crea un mega-job para un trimestre completo (90 dias), espera a que termine y descarga los reportes consolidados.

#### Bash (curl + jq)

```bash
# 1. Configura tu API key
API_KEY="TU_API_KEY"
BASE_URL="https://tu-dominio.com"

# 2. Sube tu coleccion de POIs (GeoJSON FeatureCollection)
COLLECTION_RESPONSE=$(curl -s -X POST "$BASE_URL/api/pois/collections" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "estancos-espana",
    "name": "Estancos Espana",
    "geojson": {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "geometry": { "type": "Point", "coordinates": [-3.7038, 40.4168] },
          "properties": { "id": "estanco_001", "name": "Estanco Sol" }
        },
        {
          "type": "Feature",
          "geometry": { "type": "Point", "coordinates": [-3.7050, 40.4200] },
          "properties": { "id": "estanco_002", "name": "Estanco Gran Via" }
        }
      ]
    }
  }')

echo "Coleccion creada: $(echo "$COLLECTION_RESPONSE" | jq -r '.id')"

# 3. Crea el mega-job (auto-split: divide 90 dias en sub-jobs de 31 dias)
MEGA_RESPONSE=$(curl -s -X POST "$BASE_URL/api/mega-jobs" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "auto-split",
    "name": "Estancos Espana Q1 2026",
    "country": "ES",
    "poiCollectionId": "estancos-espana",
    "dateRange": { "from": "2026-01-01", "to": "2026-03-31" },
    "radius": 10,
    "schema": "BASIC",
    "type": "pings"
  }')

MEGA_ID=$(echo "$MEGA_RESPONSE" | jq -r '.megaJob.id')
TOTAL=$(echo "$MEGA_RESPONSE" | jq -r '.splitPreview.totalSubJobs')
echo "Mega-job creado: $MEGA_ID ($TOTAL sub-jobs)"

# 4. Crea sub-jobs via polling (1 por llamada, ~30s entre llamadas)
while true; do
  POLL_RESPONSE=$(curl -s -X POST "$BASE_URL/api/mega-jobs/$MEGA_ID/create-poll" \
    -H "X-API-Key: $API_KEY")
  DONE=$(echo "$POLL_RESPONSE" | jq -r '.done')
  CREATED=$(echo "$POLL_RESPONSE" | jq -r '.megaJob.progress.created')
  echo "Sub-jobs creados: $CREATED/$TOTAL"

  if [ "$DONE" = "true" ]; then
    echo "Todos los sub-jobs creados"
    break
  fi

  sleep 30
done

# 5. Espera a que todos los sub-jobs terminen (consulta cada 5 min)
while true; do
  DETAIL=$(curl -s "$BASE_URL/api/mega-jobs/$MEGA_ID" \
    -H "X-API-Key: $API_KEY")
  STATUS=$(echo "$DETAIL" | jq -r '.status')
  SYNCED=$(echo "$DETAIL" | jq -r '.progress.synced')
  echo "Estado: $STATUS (sincronizados: $SYNCED/$TOTAL)"

  if [ "$SYNCED" = "$TOTAL" ]; then
    echo "Todos los sub-jobs sincronizados"
    break
  fi

  sleep 300  # 5 minutos
done

# 6. Consolida reportes (polling hasta phase = "done")
while true; do
  CONSOL=$(curl -s -X POST "$BASE_URL/api/mega-jobs/$MEGA_ID/consolidate" \
    -H "X-API-Key: $API_KEY")
  PHASE=$(echo "$CONSOL" | jq -r '.phase')
  MSG=$(echo "$CONSOL" | jq -r '.progress.message')
  echo "Consolidacion: $PHASE - $MSG"

  if [ "$PHASE" = "done" ]; then
    echo "Consolidacion completa"
    break
  fi

  sleep 30
done

# 7. Obtener reportes consolidados (JSON)
curl -s "$BASE_URL/api/mega-jobs/$MEGA_ID/reports?type=visits" \
  -H "X-API-Key: $API_KEY" | jq '.visitsByPoi[:3]'

# 8. Descargar reportes (CSV)
curl -s "$BASE_URL/api/mega-jobs/$MEGA_ID/reports/download?type=temporal" \
  -H "X-API-Key: $API_KEY" --output "temporal.csv"

curl -s "$BASE_URL/api/mega-jobs/$MEGA_ID/reports/download?type=catchment" \
  -H "X-API-Key: $API_KEY" --output "catchment.csv"

echo "Reportes descargados: temporal.csv, catchment.csv"
```

#### Python (requests)

```python
import requests, time

API_KEY = "TU_API_KEY"
BASE_URL = "https://tu-dominio.com"
HEADERS = {"X-API-Key": API_KEY, "Content-Type": "application/json"}

# 1. Subir coleccion de POIs
collection = requests.post(f"{BASE_URL}/api/pois/collections", headers=HEADERS, json={
    "id": "estancos-espana",
    "name": "Estancos Espana",
    "geojson": {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-3.7038, 40.4168]},
                "properties": {"id": "estanco_001", "name": "Estanco Sol"}
            }
        ]
    }
}).json()
print(f"Coleccion: {collection['id']} ({collection['poiCount']} POIs)")

# 2. Crear mega-job
mega = requests.post(f"{BASE_URL}/api/mega-jobs", headers=HEADERS, json={
    "mode": "auto-split",
    "name": "Estancos Q1 2026",
    "country": "ES",
    "poiCollectionId": "estancos-espana",
    "dateRange": {"from": "2026-01-01", "to": "2026-03-31"},
    "radius": 10,
    "schema": "BASIC",
    "type": "pings"
}).json()

mega_id = mega["megaJob"]["id"]
total = mega["splitPreview"]["totalSubJobs"]
print(f"Mega-job: {mega_id} ({total} sub-jobs)")

# 3. Crear sub-jobs (polling)
while True:
    poll = requests.post(f"{BASE_URL}/api/mega-jobs/{mega_id}/create-poll", headers=HEADERS).json()
    created = poll["megaJob"]["progress"]["created"]
    print(f"  Sub-jobs creados: {created}/{total}")
    if poll.get("done"):
        break
    time.sleep(30)

# 4. Esperar sincronizacion
while True:
    detail = requests.get(f"{BASE_URL}/api/mega-jobs/{mega_id}", headers=HEADERS).json()
    synced = detail["progress"]["synced"]
    print(f"  Sincronizados: {synced}/{total}")
    if synced >= total:
        break
    time.sleep(300)

# 5. Consolidar
while True:
    consol = requests.post(f"{BASE_URL}/api/mega-jobs/{mega_id}/consolidate", headers=HEADERS).json()
    print(f"  Consolidacion: {consol['phase']} - {consol['progress']['message']}")
    if consol["phase"] == "done":
        break
    time.sleep(30)

# 6. Obtener reportes
visits = requests.get(f"{BASE_URL}/api/mega-jobs/{mega_id}/reports?type=visits", headers=HEADERS).json()
for v in visits["visitsByPoi"][:5]:
    print(f"  {v['poiName']}: {v['visits']} visitas, {v['devices']} dispositivos")

# 7. Descargar CSV
for report_type in ["temporal", "catchment", "od", "hourly"]:
    resp = requests.get(f"{BASE_URL}/api/mega-jobs/{mega_id}/reports/download?type={report_type}", headers=HEADERS)
    with open(f"{report_type}.csv", "w") as f:
        f.write(resp.text)
    print(f"  Descargado: {report_type}.csv")
```

---

### 9. Subir Coleccion de POIs

**`POST /api/pois/collections`**

Sube una coleccion GeoJSON FeatureCollection para usar con mega-jobs. Cada POI debe ser un Feature con geometria de tipo Point. Las features invalidas (coordenadas fuera de rango, geometrias no-Point) se filtran automaticamente.

> **Nota:** Las colecciones se almacenan en S3. El sistema valida las coordenadas y reporta cuantas features fueron validas vs invalidas.

#### Cuerpo de la Peticion

| Campo | Tipo | Requerido | Descripcion |
|-------|------|-----------|-------------|
| `id` | string | No | Identificador unico de la coleccion. Si no se provee, se genera automaticamente desde `name` (kebab-case). |
| `name` | string | Si | Nombre legible de la coleccion |
| `description` | string | No | Descripcion opcional |
| `geojson` | object | Si | GeoJSON FeatureCollection con POIs de tipo Point |
| `geojson.features[].properties.id` | string | Recomendado | Identificador unico del POI. Si no se provee, se genera como `poi_N`. |
| `geojson.features[].properties.name` | string | No | Nombre legible del POI |

#### Ejemplo de Peticion

```bash
curl -X POST https://tu-dominio.com/api/pois/collections \
  -H "X-API-Key: TU_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "estancos-espana",
    "name": "Estancos Espana",
    "geojson": {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "geometry": { "type": "Point", "coordinates": [-3.7038, 40.4168] },
          "properties": { "id": "estanco_001", "name": "Estanco Sol" }
        },
        {
          "type": "Feature",
          "geometry": { "type": "Point", "coordinates": [-3.7050, 40.4200] },
          "properties": { "id": "estanco_002", "name": "Estanco Gran Via" }
        }
      ]
    }
  }'
```

#### Respuesta (200 OK)

```json
{
  "id": "estancos-espana",
  "name": "Estancos Espana",
  "description": "",
  "poiCount": 2,
  "totalFeatures": 2,
  "invalidFeatures": 0,
  "geojsonPath": "pois/estancos-espana.geojson",
  "createdAt": "2026-03-20T10:00:00.000Z"
}
```

> **Nota:** El campo `poiCount` refleja el numero de features **validas** (Point con coordenadas correctas). Si `invalidFeatures > 0`, algunas features fueron filtradas. El campo `totalFeatures` muestra el total original.

---

### 10. Listar Colecciones de POIs

**`GET /api/pois/collections`**

Lista todas las colecciones de POIs disponibles, ordenadas por fecha de creacion (mas recientes primero).

#### Ejemplo de Peticion

```bash
curl -H "X-API-Key: TU_API_KEY" \
  https://tu-dominio.com/api/pois/collections
```

#### Respuesta (200 OK)

```json
[
  {
    "id": "estancos-espana",
    "name": "Estancos Espana",
    "poiCount": 150,
    "totalFeatures": 152,
    "invalidFeatures": 2,
    "geojsonPath": "pois/estancos-espana.geojson",
    "createdAt": "2026-03-20T10:00:00.000Z"
  }
]
```

---

### 11. Crear Mega-Job (Auto-Split)

**`POST /api/mega-jobs`**

Crea un mega-job que divide automaticamente un rango de fechas en sub-jobs de maximo 31 dias. Si el rango total es menor a 31 dias, se crea un unico sub-job. El sistema tambien divide por lotes de POIs si la coleccion es grande.

#### Modos de Creacion

| Modo | Descripcion |
|------|-------------|
| `auto-split` | Division automatica del rango de fechas y POIs. Requiere `poiCollectionId`. |
| `manual-group` | Agrupa sub-jobs existentes (ya completados). Requiere `subJobIds`. |

#### Cuerpo de la Peticion (modo `auto-split`)

| Campo | Tipo | Requerido | Descripcion |
|-------|------|-----------|-------------|
| `mode` | string | Si | Debe ser `"auto-split"` |
| `name` | string | Si | Nombre del mega-job (1-200 caracteres) |
| `description` | string | No | Descripcion opcional (max 1000 caracteres) |
| `country` | string | No | Codigo ISO 2 letras del pais (ej: `"ES"`, `"MX"`) |
| `poiCollectionId` | string | Si | ID de la coleccion POI subida previamente |
| `dateRange.from` | string | Si | Fecha inicio `YYYY-MM-DD` |
| `dateRange.to` | string | Si | Fecha fin `YYYY-MM-DD` (sin limite maximo) |
| `radius` | number | No | Radio en metros (1-1000). Default: `10` |
| `schema` | string | No | `"BASIC"`, `"ENHANCED"` o `"FULL"`. Default: `"BASIC"` |
| `type` | string | No | `"pings"`, `"devices"`, `"aggregate"`, `"cohort"` o `"pings_by_device"`. Default: `"pings"` |

#### Ejemplo de Peticion

```bash
curl -X POST https://tu-dominio.com/api/mega-jobs \
  -H "X-API-Key: TU_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "auto-split",
    "name": "Tabaco Espana Q1 2026",
    "country": "ES",
    "poiCollectionId": "estancos-espana",
    "dateRange": { "from": "2026-01-01", "to": "2026-03-31" },
    "radius": 10,
    "schema": "BASIC",
    "type": "pings"
  }'
```

#### Respuesta (201 Created)

```json
{
  "megaJob": {
    "id": "mj_abc123",
    "name": "Tabaco Espana Q1 2026",
    "mode": "auto-split",
    "status": "planning",
    "country": "ES",
    "sourceScope": {
      "poiCollectionId": "estancos-espana",
      "dateRange": { "from": "2026-01-01", "to": "2026-03-31" },
      "radius": 10,
      "schema": "BASIC",
      "type": "pings"
    },
    "progress": { "created": 0, "synced": 0, "failed": 0, "total": 3 },
    "subJobIds": [],
    "createdAt": "2026-03-20T10:05:00.000Z"
  },
  "splitPreview": {
    "dateChunks": [
      { "from": "2026-01-01", "to": "2026-01-31" },
      { "from": "2026-02-01", "to": "2026-03-03" },
      { "from": "2026-03-04", "to": "2026-03-31" }
    ],
    "poiChunks": [
      { "startIndex": 0, "endIndex": 150, "label": "POIs 1-150" }
    ],
    "totalSubJobs": 3,
    "poiCollectionName": "Estancos Espana",
    "totalPois": 150,
    "quotaRemaining": 197
  }
}
```

> **Nota:** El `splitPreview` muestra como el sistema dividira el rango de fechas. Cada `dateChunk` sera un sub-job independiente enviado a Veraset. Si la coleccion tiene muchos POIs, tambien se dividira en `poiChunks`, resultando en `dateChunks x poiChunks` sub-jobs totales.

#### Respuestas de Error

```json
// 400 - Modo invalido
{
  "error": "Invalid mode. Must be \"auto-split\" or \"manual-group\"."
}

// 404 - Coleccion no encontrada
{
  "error": "POI collection \"mi-coleccion\" not found"
}

// 429 - Limite de cuota alcanzado
{
  "error": "Monthly job creation limit exceeded",
  "remaining": 0
}
```

---

### 12. Crear Sub-Jobs (Polling)

**`POST /api/mega-jobs/:megaJobId/create-poll`**

Crea **un sub-job por llamada** a traves de la API de Veraset. El frontend o script debe llamar repetidamente a este endpoint hasta que la respuesta contenga `done: true`.

En la primera llamada, el mega-job transiciona de `planning` a `creating`. Cuando se crea el ultimo sub-job, transiciona a `running`.

> **Importante:** Solo funciona para mega-jobs en modo `auto-split`. Los mega-jobs `manual-group` no necesitan este paso ya que los sub-jobs ya existen.

#### Ejemplo de Peticion

```bash
curl -X POST https://tu-dominio.com/api/mega-jobs/mj_abc123/create-poll \
  -H "X-API-Key: TU_API_KEY"
```

#### Respuesta - Sub-Job Creado

```json
{
  "megaJob": {
    "id": "mj_abc123",
    "status": "creating",
    "progress": { "created": 1, "synced": 0, "failed": 0, "total": 3 }
  },
  "createdJobId": "veraset_job_456",
  "subJobName": "Tabaco Espana Q1 2026 [2026-01-01→2026-01-31]",
  "done": false
}
```

#### Respuesta - Todos los Sub-Jobs Creados

```json
{
  "megaJob": {
    "id": "mj_abc123",
    "status": "running",
    "progress": { "created": 3, "synced": 0, "failed": 0, "total": 3 }
  },
  "createdJobId": "veraset_job_789",
  "subJobName": "Tabaco Espana Q1 2026 [2026-03-04→2026-03-31]",
  "done": true
}
```

#### Respuesta - Sub-Job Ya Existente (todos creados)

```json
{
  "megaJob": {
    "id": "mj_abc123",
    "status": "running",
    "progress": { "created": 3, "synced": 0, "failed": 0, "total": 3 }
  },
  "message": "All sub-jobs already created",
  "done": true
}
```

#### Respuesta - Error en un Sub-Job (no detiene el mega-job)

```json
{
  "megaJob": {
    "id": "mj_abc123",
    "status": "creating",
    "progress": { "created": 2, "synced": 0, "failed": 1, "total": 3 }
  },
  "subJobError": "Veraset 400: Invalid date range",
  "done": false
}
```

> **Nota:** Si un sub-job individual falla al crearse en Veraset, el mega-job **no se detiene**. El error se registra en `progress.failed` y el sistema avanza al siguiente sub-job. Puedes verificar cuantos fallaron en el detalle del mega-job.

---

### 13. Detalle de Mega-Job

**`GET /api/mega-jobs/:megaJobId`**

Obtiene el detalle completo del mega-job, incluyendo el estado actualizado de cada sub-job. Este endpoint **actualiza automaticamente** el progreso del mega-job basandose en el estado actual de los sub-jobs en Veraset.

#### Ejemplo de Peticion

```bash
curl -H "X-API-Key: TU_API_KEY" \
  https://tu-dominio.com/api/mega-jobs/mj_abc123
```

#### Respuesta

```json
{
  "id": "mj_abc123",
  "name": "Tabaco Espana Q1 2026",
  "mode": "auto-split",
  "status": "running",
  "country": "ES",
  "sourceScope": {
    "poiCollectionId": "estancos-espana",
    "dateRange": { "from": "2026-01-01", "to": "2026-03-31" },
    "radius": 10,
    "schema": "BASIC",
    "type": "pings"
  },
  "progress": { "created": 3, "synced": 2, "failed": 0, "total": 3 },
  "subJobIds": ["veraset_job_456", "veraset_job_567", "veraset_job_789"],
  "subJobs": [
    {
      "jobId": "veraset_job_456",
      "name": "Tabaco Espana Q1 2026 [2026-01-01→2026-01-31]",
      "status": "SUCCESS",
      "dateRange": { "from": "2026-01-01", "to": "2026-01-31" },
      "poiCount": 150,
      "syncedAt": "2026-03-20T14:00:00.000Z",
      "megaJobIndex": 0
    },
    {
      "jobId": "veraset_job_567",
      "name": "Tabaco Espana Q1 2026 [2026-02-01→2026-03-03]",
      "status": "SUCCESS",
      "dateRange": { "from": "2026-02-01", "to": "2026-03-03" },
      "poiCount": 150,
      "syncedAt": "2026-03-20T14:15:00.000Z",
      "megaJobIndex": 1
    },
    {
      "jobId": "veraset_job_789",
      "name": "Tabaco Espana Q1 2026 [2026-03-04→2026-03-31]",
      "status": "RUNNING",
      "dateRange": { "from": "2026-03-04", "to": "2026-03-31" },
      "poiCount": 150,
      "syncedAt": null,
      "megaJobIndex": 2
    }
  ],
  "createdAt": "2026-03-20T10:05:00.000Z"
}
```

#### Estados del Mega-Job

| Estado | Descripcion | Accion recomendada |
|--------|-------------|-------------------|
| `planning` | Mega-job creado, sub-jobs aun no enviados | Llamar a `create-poll` para empezar |
| `creating` | Sub-jobs enviandose a Veraset (en progreso) | Seguir llamando a `create-poll` cada 30s |
| `running` | Todos los sub-jobs creados, esperando resultados | Consultar detalle cada 5 min |
| `consolidating` | Consolidacion de reportes en progreso | Esperar a que `consolidate` retorne `phase: "done"` |
| `completed` | Todos los reportes consolidados disponibles | Consultar reportes via `reports` |
| `partial` | Algunos sub-jobs fallaron, pero hay resultados parciales | Consolidar los sub-jobs exitosos |
| `error` | Todos los sub-jobs fallaron | Diagnosticar y recrear |

---

### 14. Listar Mega-Jobs

**`GET /api/mega-jobs`**

Lista todos los mega-jobs con un resumen ligero (sin detalle de sub-jobs).

#### Ejemplo de Peticion

```bash
curl -H "X-API-Key: TU_API_KEY" \
  https://tu-dominio.com/api/mega-jobs
```

#### Respuesta (200 OK)

```json
[
  {
    "id": "mj_abc123",
    "name": "Tabaco Espana Q1 2026",
    "mode": "auto-split",
    "status": "completed",
    "country": "ES",
    "progress": { "created": 3, "synced": 3, "failed": 0, "total": 3 },
    "createdAt": "2026-03-20T10:05:00.000Z"
  }
]
```

---

### 15. Consolidar Reportes

**`POST /api/mega-jobs/:megaJobId/consolidate`**

Consolida los datos de todos los sub-jobs completados en reportes unificados. El proceso es multi-fase con queries Athena paralelos. Este endpoint es **idempotente**: puedes llamarlo repetidamente hasta que `phase` sea `"done"`.

#### Cuerpo de la Peticion (opcional)

| Campo | Tipo | Requerido | Descripcion |
|-------|------|-----------|-------------|
| `poiIds` | string[] | No | Array de POI IDs para filtrar. Si se omite, se incluyen todos los POIs. |

#### Query Params

| Param | Descripcion |
|-------|-------------|
| `reset=true` | Reinicia la consolidacion desde cero (util si una fase fallo). |

#### Ejemplo de Peticion

```bash
# Consolidar todos los POIs
curl -X POST https://tu-dominio.com/api/mega-jobs/mj_abc123/consolidate \
  -H "X-API-Key: TU_API_KEY"

# Consolidar solo POIs especificos
curl -X POST https://tu-dominio.com/api/mega-jobs/mj_abc123/consolidate \
  -H "X-API-Key: TU_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{ "poiIds": ["estanco_001", "estanco_002"] }'

# Reiniciar consolidacion
curl -X POST "https://tu-dominio.com/api/mega-jobs/mj_abc123/consolidate?reset=true" \
  -H "X-API-Key: $API_KEY"
```

#### Fases de Consolidacion

La consolidacion se ejecuta en 5 fases. Cada llamada al endpoint avanza una fase (o espera si la fase actual no ha terminado):

| Fase | Nombre | Descripcion |
|------|--------|-------------|
| 1 | `starting` | Inicia 7 queries Athena en paralelo (visits, OD, hourly, catchment, mobility, temporal, MAIDs) |
| 2 | `polling` | Espera a que todos los queries completen en Athena |
| 3 | `parsing_visits` | Parsea los resultados de visits, hourly, mobility y temporal (sin geocodificacion) |
| 4 | `parsing_od` | Parsea OD y catchment con reverse geocodificacion (puede tardar mas) |
| 5 | `done` | Todos los reportes guardados y disponibles |

#### Respuesta - Fase `starting`

```json
{
  "phase": "polling",
  "progress": {
    "step": "queries_started",
    "percent": 10,
    "message": "Started 7 Athena queries..."
  }
}
```

#### Respuesta - Fase `polling`

```json
{
  "phase": "polling",
  "progress": {
    "step": "polling_queries",
    "percent": 25,
    "message": "Queries: 3 done, 4 running..."
  },
  "statuses": {
    "visits": "SUCCEEDED",
    "od": "RUNNING",
    "hourly": "SUCCEEDED",
    "catchment": "RUNNING",
    "mobility": "RUNNING",
    "temporal": "SUCCEEDED",
    "maids": "RUNNING"
  }
}
```

#### Respuesta - Fase `parsing_od`

```json
{
  "phase": "parsing_od",
  "progress": {
    "step": "parsing_geocode",
    "percent": 60,
    "message": "Geocoding origins and destinations..."
  }
}
```

#### Respuesta - Fase `done`

```json
{
  "phase": "done",
  "progress": {
    "step": "complete",
    "percent": 100,
    "message": "Consolidation complete"
  }
}
```

---

### 16. Obtener Reportes Consolidados (JSON)

**`GET /api/mega-jobs/:megaJobId/reports?type={tipo}`**

Obtiene un reporte consolidado en formato JSON. Requiere que la consolidacion haya completado (`phase: "done"`).

#### Tipos de Reporte

| Tipo | Descripcion |
|------|-------------|
| `visits` | Visitas y dispositivos por POI |
| `temporal` | Tendencia diaria (pings y dispositivos por dia) |
| `catchment` | Origenes residenciales por codigo postal |
| `od` | Analisis origen-destino |
| `hourly` | Distribucion horaria de actividad |
| `mobility` | Categorias de POIs visitados antes/despues |

#### Ejemplo de Peticion

```bash
curl -H "X-API-Key: TU_API_KEY" \
  "https://tu-dominio.com/api/mega-jobs/mj_abc123/reports?type=visits"
```

#### Respuesta - Reporte `visits`

```json
{
  "megaJobId": "mj_abc123",
  "analyzedAt": "2026-03-20T15:00:00.000Z",
  "totalPois": 150,
  "visitsByPoi": [
    { "poiId": "estanco_001", "poiName": "Estanco Sol", "visits": 15432, "devices": 3456 },
    { "poiId": "estanco_002", "poiName": "Estanco Gran Via", "visits": 12100, "devices": 2890 }
  ]
}
```

#### Respuesta - Reporte `temporal`

```json
{
  "megaJobId": "mj_abc123",
  "analyzedAt": "2026-03-20T15:00:00.000Z",
  "daily": [
    { "date": "2026-01-01", "pings": 45000, "devices": 3200 },
    { "date": "2026-01-02", "pings": 52000, "devices": 3800 }
  ]
}
```

#### Respuesta - Reporte `catchment`

```json
{
  "megaJobId": "mj_abc123",
  "analyzedAt": "2026-03-20T15:00:00.000Z",
  "byZipCode": [
    { "zipCode": "28001", "city": "Madrid", "country": "ES", "lat": 40.42, "lng": -3.70, "deviceDays": 1234 },
    { "zipCode": "08001", "city": "Barcelona", "country": "ES", "lat": 41.38, "lng": 2.17, "deviceDays": 987 }
  ]
}
```

#### Respuesta - Reporte `od`

```json
{
  "megaJobId": "mj_abc123",
  "analyzedAt": "2026-03-20T15:00:00.000Z",
  "origins": [
    { "zipCode": "28001", "city": "Madrid", "country": "ES", "lat": 40.42, "lng": -3.70, "deviceDays": 1234 }
  ],
  "destinations": [
    { "zipCode": "28006", "city": "Madrid", "country": "ES", "lat": 40.43, "lng": -3.68, "deviceDays": 1100 }
  ]
}
```

#### Respuesta - Reporte `hourly`

```json
{
  "megaJobId": "mj_abc123",
  "analyzedAt": "2026-03-20T15:00:00.000Z",
  "hourly": [
    { "hour": 8, "pings": 12000, "devices": 2500 },
    { "hour": 9, "pings": 18000, "devices": 3200 },
    { "hour": 13, "pings": 15000, "devices": 2800 }
  ]
}
```

#### Respuesta - Reporte `mobility`

```json
{
  "megaJobId": "mj_abc123",
  "analyzedAt": "2026-03-20T15:00:00.000Z",
  "before": [
    { "category": "restaurant", "deviceDays": 450, "hits": 890 },
    { "category": "cafe", "deviceDays": 320, "hits": 510 }
  ],
  "after": [
    { "category": "shopping_mall", "deviceDays": 380, "hits": 650 },
    { "category": "parking", "deviceDays": 250, "hits": 420 }
  ],
  "categories": [
    { "category": "restaurant", "deviceDays": 780, "hits": 1420 },
    { "category": "shopping_mall", "deviceDays": 550, "hits": 980 }
  ]
}
```

---

### 17. Descargar Reportes Consolidados (CSV)

**`GET /api/mega-jobs/:megaJobId/reports/download?type={tipo}`**

Descarga un reporte consolidado en formato CSV. Util para importar en Excel, Tableau u otras herramientas de analisis.

#### Tipos de Descarga

| Tipo | Columnas CSV | Descripcion |
|------|-------------|-------------|
| `visits` | `poi_id,poi_name,visits,devices` | Visitas por POI |
| `temporal` | `date,pings,devices` | Tendencia diaria |
| `catchment` | `zip_code,city,country,lat,lng,device_days` | Origenes residenciales |
| `od` | `type,zip_code,city,country,lat,lng,device_days` | Origen-destino (type = origin/destination) |
| `hourly` | `hour,pings,devices` | Distribucion horaria |
| `mobility` | `timing,category,device_days,hits` | Movilidad (timing = before/after/combined) |
| `maids` | `maid` | Identificadores de dispositivo (acceso restringido) |
| `postcodes` | `postal_code,device_days` | Codigos postales limpios (solo numericos) |

#### Ejemplo de Peticion

```bash
# Descargar reporte de visitas
curl -H "X-API-Key: TU_API_KEY" \
  "https://tu-dominio.com/api/mega-jobs/mj_abc123/reports/download?type=visits" \
  --output "visitas.csv"

# Descargar tendencia temporal
curl -H "X-API-Key: TU_API_KEY" \
  "https://tu-dominio.com/api/mega-jobs/mj_abc123/reports/download?type=temporal" \
  --output "temporal.csv"

# Descargar catchment
curl -H "X-API-Key: TU_API_KEY" \
  "https://tu-dominio.com/api/mega-jobs/mj_abc123/reports/download?type=catchment" \
  --output "catchment.csv"

# Descargar MAIDs (acceso restringido)
curl -H "X-API-Key: TU_API_KEY" \
  "https://tu-dominio.com/api/mega-jobs/mj_abc123/reports/download?type=maids" \
  --output "maids.csv"
```

#### Headers de Respuesta

| Header | Valor |
|--------|-------|
| `Content-Type` | `text/csv` |
| `Content-Disposition` | `attachment; filename="mega-job-{id}-{tipo}.csv"` |

---

### Flujo Tipico de Mega-Job

```
Subir POIs          Crear             Crear Sub-Jobs      Monitorear          Consolidar         Reportes
──────────          ─────             ──────────────      ──────────          ──────────         ────────

POST /pois/     POST /mega-jobs   POST /mega-jobs/   GET /mega-jobs/    POST /mega-jobs/   GET /mega-jobs/
 collections        |               :id/create-poll    :id                :id/consolidate    :id/reports
    |               |                    |                 |                    |               ?type=visits
    |  id           |  megaJob.id        | done=false      | synced < total    | phase!=done     |
    |<──────        |<─────────          |<──── loop ───>  |<──── loop ──────> |<── loop ────>   |
    |               |                    |                 |                    |               GET /mega-jobs/
    |               |                    | done=true       | synced == total   | phase=done     :id/reports/
    |               |                    |─────────────>   |────────────────>  |─────────────>  download
    |               |                                                                          ?type=temporal
```

### Paso a Paso

1. **`POST /api/pois/collections`** — Subir coleccion de POIs en formato GeoJSON.
2. **`POST /api/mega-jobs`** — Crear mega-job con `mode: "auto-split"`, recibir `megaJob.id` y preview de la division.
3. **`POST /api/mega-jobs/:id/create-poll`** — Llamar en loop (cada 30-60 segundos) hasta `done: true`. Cada llamada crea un sub-job en Veraset.
4. **`GET /api/mega-jobs/:id`** — Consultar periodicamente (cada 5 minutos) hasta que `progress.synced == progress.total`.
5. **`POST /api/mega-jobs/:id/consolidate`** — Llamar en loop (cada 30-60 segundos) hasta `phase: "done"`. El sistema ejecuta queries Athena y geocodificacion en paralelo.
6. **`GET /api/mega-jobs/:id/reports?type=visits`** — Obtener reportes consolidados en JSON.
7. **`GET /api/mega-jobs/:id/reports/download?type=temporal`** — Descargar reportes en formato CSV.

### Intervalos de Polling Recomendados

| Operacion | Intervalo | Razon |
|-----------|-----------|-------|
| `create-poll` | 30-60 segundos | Cada llamada crea un job en Veraset (~5-20s) |
| Detalle mega-job | 5 minutos | Los sub-jobs tardan horas en completarse |
| `consolidate` | 30-60 segundos | Los queries Athena tardan 1-5 minutos |

### Tiempos Tipicos de Mega-Jobs

| Operacion | Tiempo tipico |
|-----------|---------------|
| Subir coleccion (150 POIs) | < 2 segundos |
| Crear mega-job | < 2 segundos |
| Crear todos los sub-jobs (3 sub-jobs) | 1-3 minutos |
| Procesamiento de sub-jobs (Veraset) | 2-8 horas |
| Consolidacion completa | 3-15 minutos |
| Descarga de reportes CSV | < 2 segundos |

---

## Codigos de Error

| HTTP Status | Error | Descripcion | Que hacer |
|-------------|-------|-------------|-----------|
| 400 | Validation Error | El body fallo la validacion. El mensaje indica los campos con error. | Revisar el campo `error` — incluye la ruta del campo y el problema especifico. |
| 400 | Invalid POI ID | El POI ID no existe en el job (para endpoints per-POI). | Revisar el campo `available_poi_ids` en la respuesta para ver los IDs validos. |
| 401 | Unauthorized | API key faltante o invalida | Verificar que envias `X-API-Key` header o `api_key` query param con una key valida. |
| 404 | Not Found | Job ID o dataset name no encontrado | Verificar el ID. Usa "Listar Jobs" para ver los disponibles. |
| 409 | Job Not Ready | Job no completado aun (para catchment/OD) | Esperar a que el status sea `SUCCESS` antes de pedir catchment u OD. |
| 409 | Not Synced | Job completado pero datos aun transfiriendose | Reintentar en 1-2 minutos. La sincronizacion es automatica. |
| 409 | Dataset not accessible | Datos del job aun no sincronizados en S3 | Reintentar en unos minutos. Puede indicar problemas de permisos AWS. |
| 429 | Rate Limited | Demasiadas peticiones en la ventana de 1 minuto | Esperar segun header `Retry-After`. Limite: 100 req/min por ruta. |
| 429 | Limit Reached | Limite mensual de creacion de jobs alcanzado (200/mes default) | Contactar al administrador para aumentar el limite. |
| 502 | Upstream Error | La llamada a la API de Veraset fallo | Error transitorio. Reintentar despues de unos segundos. |
| 503 | Configuration Error | Credenciales AWS no configuradas en el servidor | Contactar al administrador. |
| 500 | Internal Error | Error inesperado del servidor | Reportar al administrador con el `job_id` si es posible. |
| 400 | No synced sub-jobs | Intentaste consolidar sin sub-jobs sincronizados | Esperar a que al menos un sub-job tenga status `SUCCESS` y `syncedAt`. |
| 400 | Only auto-split | Llamaste `create-poll` en un mega-job `manual-group` | Los mega-jobs `manual-group` no necesitan `create-poll`. |
| 400 | Invalid type | Tipo de reporte no valido en `reports` o `download` | Usar uno de: visits, temporal, catchment, od, hourly, mobility, maids, postcodes. |
| 404 | Report not found | Reporte no consolidado aun | Ejecutar consolidacion antes de solicitar reportes. |
| 504 | Veraset API timeout | La llamada a Veraset excedio 40 segundos al crear sub-job | Reintentar `create-poll` en unos segundos. |

### Formato de Errores de Validacion (400)

Los errores de validacion incluyen detalle por campo:

```json
{
  "error": "Validation error: name: String must contain at least 1 character(s), date_range: Date range must be between 1 and 31 days (received: {\"from\":\"2026-01-01\",\"to\":\"2026-03-01\"})"
}
```

### Error de POI ID Invalido (400)

Cuando el POI ID no existe en el job, la respuesta incluye los IDs disponibles:

```json
{
  "error": "Invalid POI ID",
  "message": "POI 'estanco_999' not found in this job.",
  "available_poi_ids": ["estanco_001", "estanco_002", "estanco_003"]
}
```

---

## Flujo Tipico de Uso

```
Crear                Monitorear               Analizar
─────                ──────────               ────────

POST /jobs ─────> GET /jobs/:id/status ──┬──> GET /jobs/:id/catchment          (global)
  |                    |        |        ├──> GET /jobs/:id/catchment/:poiId   (por POI)
  |  job_id            | QUEUED |        ├──> GET /jobs/:id/od                 (global)
  |<───────            | SCHEDULED       ├──> GET /jobs/:id/od/:poiId          (por POI)
  |                    | RUNNING|        ├──> GET /jobs/:id/mobility/:poiId    (categorias antes/despues)
  |  webhook?          | (auto- |        ├──> GET /jobs/:name/analysis         (metricas diarias)
  |                    |  check)|        └──> POST /jobs/:id/postal-maid      (MAIDs por CP)
  |  ─ ─ ─ ─ ─ ─ ─ ─> |  check)|
  |                    v        |          Standalone
  |               SUCCESS ──────┘          ─────────
  |               (auto-sync)              POST /postal-maid                   (MAIDs por pais)
  |
  └─── POST /jobs/poll ───────────────> (batch: verifica todos los pendientes)
```

### Paso a Paso

1. **`POST /api/external/jobs`** — Crear job, recibir `job_id`
2. **`GET /api/external/jobs/:jobId/status`** — Consultar periodicamente (cada 5 min) hasta que `status = "SUCCESS"`. Alternativamente, registrar un `webhook_url` y esperar la notificacion. Opcionalmente, usar **`POST /api/external/jobs/poll`** para verificar multiples jobs de una sola vez.
3. Cuando `status = "SUCCESS"` y `synced = true`:
   - Los **resultados basicos** (pings, dispositivos, resumen por POI) ya estan en la respuesta de status.
   - **`GET /api/external/jobs/:jobId/catchment`** — Origenes residenciales de todos los POIs.
   - **`GET /api/external/jobs/:jobId/catchment/:poiId`** — Origenes residenciales de un POI especifico.
   - **`GET /api/external/jobs/:jobId/od`** — Origen-destino de todos los POIs.
   - **`GET /api/external/jobs/:jobId/od/:poiId`** — Origen-destino de un POI especifico.
   - **`GET /api/external/jobs/:jobId/mobility/:poiId`** — Categorias de POIs visitados antes/despues de un POI especifico.
   - **`GET /api/external/jobs/:datasetName/analysis`** — Metricas diarias y POIs activos.
   - **`POST /api/external/jobs/:jobId/postal-maid`** — Exportar MAIDs por codigos postales (acceso restringido).
4. **`POST /api/external/postal-maid`** — Exportar MAIDs por pais sin necesidad de un job (acceso restringido).

### Tiempos Tipicos

| Operacion | Tiempo tipico |
|-----------|---------------|
| Creacion del job | Instantaneo (< 2s) |
| Procesamiento del job (QUEUED -> SUCCESS) | 1-4 horas |
| Primera peticion de catchment (global) | 1-5 minutos |
| Primera peticion de catchment (por POI) | 1-3 minutos |
| Catchment cacheado (peticiones posteriores) | < 1 segundo |
| Primera peticion de OD (global) | 1-5 minutos |
| Primera peticion de OD (por POI) | 1-3 minutos |
| OD cacheado (peticiones posteriores) | < 1 segundo |
| Primera peticion de mobility/categorias (por POI) | 1-3 minutos |
| Mobility cacheado (peticiones posteriores) | < 1 segundo |
| Analisis diario (primera peticion) | 30-90 segundos |
| Analisis diario cacheado | < 1 segundo |
| Postal-MAID (primera peticion) | 1-5 minutos |
| Postal-MAID cacheado | < 1 segundo |
| Batch poll (todos los jobs pendientes) | 10-300 segundos (depende de cantidad) |

### Intervalo de Polling Recomendado

**Cada 5 minutos.** No hay beneficio en consultar mas frecuentemente ya que los jobs tardan horas. El endpoint de status verifica automaticamente con Veraset en cada llamada, asi que siempre obtienes el estado mas reciente.

### Cuando Usar Mega-Jobs vs Jobs Regulares

| Criterio | Job Regular | Mega-Job |
|----------|-------------|----------|
| Rango de fechas | Maximo 31 dias | Sin limite (se divide automaticamente) |
| POIs | Hasta 25,000 en el body | Coleccion GeoJSON ilimitada (se divide en lotes) |
| Reportes | Catchment, OD, mobility per-POI | Consolidados: visits, temporal, catchment, OD, hourly, mobility |
| Descarga CSV | No incluido | Incluido para todos los tipos |
| Caso de uso | Analisis puntual, rango corto | Analisis trimestral/anual, grandes colecciones |

> **Nota:** Los mega-jobs usan los mismos endpoints de Veraset internamente. La consolidacion agrega los resultados de todos los sub-jobs en reportes unificados usando queries Athena.
