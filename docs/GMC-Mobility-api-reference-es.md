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
- **Solo devuelven ad_ids anonimizados** asociados a un job especifico
- **Estan sujetos a los terminos contractuales** con el proveedor de datos

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
| `country` | string | Si | Codigo de pais ISO 2 letras (ej: `"ES"`, `"MX"`, `"US"`) |
| `type` | string | No | Tipo de job (ver tabla abajo). Default: `"pings"` |
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
  "pois": [
    { "id": "estanco_001", "name": "Estanco Sol", "latitude": 40.4168, "longitude": -3.7038 },
    { "id": "estanco_002", "name": "Estanco Gran Via", "latitude": 40.4200, "longitude": -3.7050 }
  ]
}
```

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
  "pois": [...]
}
```

#### Estados del Job

| Estado | Descripcion | Accion recomendada |
|--------|-------------|-------------------|
| `QUEUED` | Job enviado, esperando ser procesado | Seguir consultando cada 5 min |
| `RUNNING` | Job en proceso activo | Seguir consultando cada 5 min |
| `SUCCESS` | Job completado, resultados disponibles | Leer `results`, pedir catchment/OD |
| `FAILED` | Job fallo, revisar campo `error` | Diagnosticar y re-enviar si es necesario |

---

### 3. Obtener Analisis de Catchment (Origenes Residenciales)

**`GET /api/external/jobs/:jobId/catchment`**
**`GET /api/external/jobs/:jobId/catchment/:poiId`**

Determina **donde viven los visitantes** de tus POIs. Para cada dispositivo que visito un POI, se toma el primer ping GPS de cada dia (que representa su ubicacion de origen/residencia) y se hace reverse geocoding al codigo postal mas cercano.

Solo disponible para jobs con estado `SUCCESS` y datos sincronizados.

> **Importante:** La primera peticion ejecuta consultas Athena y reverse geocoding, lo cual puede tardar **1-5 minutos**. Los resultados se cachean automaticamente — las peticiones posteriores son instantaneas.

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

### 4. Obtener Analisis Origen-Destino

**`GET /api/external/jobs/:jobId/od`**
**`GET /api/external/jobs/:jobId/od/:poiId`**

Analisis completo de **de donde vienen y a donde van** los visitantes de tus POIs. Para cada dispositivo que visito un POI en un dia dado:
- **Origen** = primer ping GPS del dia (de donde venia el visitante)
- **Destino** = ultimo ping GPS del dia (a donde fue despues)

Ambos se reverse geocodean a codigo postal y se agregan.

Solo disponible para jobs con estado `SUCCESS` y datos sincronizados.

> **Importante:** La primera peticion puede tardar **1-5 minutos** (consultas Athena + reverse geocoding). Los resultados se cachean automaticamente.

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
    { "hour": 9, "deviceDays": 3200, "percentOfTotal": 10.4 },
    { "hour": 12, "deviceDays": 2800, "percentOfTotal": 9.1 },
    { "hour": 13, "deviceDays": 3100, "percentOfTotal": 10.1 },
    { "hour": 17, "deviceDays": 2900, "percentOfTotal": 9.4 },
    { "hour": 18, "deviceDays": 3400, "percentOfTotal": 11.0 }
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
  "temporal_patterns": [...]
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
| `temporal_patterns[]` | Patrones de llegada por hora UTC (ver tabla abajo) |

#### Campos de `temporal_patterns[]`

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| `hour` | number | Hora del dia UTC (0-23) |
| `deviceDays` | number | Numero de pares dispositivo-dia con primera visita al POI en esta hora |
| `percentOfTotal` | number | Porcentaje sobre el total de device-days |

---

### 5. Obtener Analisis del Dataset

**`GET /api/external/jobs/:datasetName/analysis`**

Obtiene metricas diarias del dataset: pings por dia, dispositivos por dia, y la lista de POIs activos con coordenadas.

> **Nota:** Este endpoint usa el `datasetName` (no el `jobId`). Puedes obtener el `datasetName` del endpoint "Listar Jobs" (seccion 6).

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

### 6. Listar Jobs Disponibles

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
| 429 | Limit Reached | Limite mensual de creacion de jobs alcanzado | Contactar al administrador para aumentar el limite. |
| 502 | Upstream Error | La llamada a la API de Veraset fallo | Error transitorio. Reintentar despues de unos segundos. |
| 503 | Configuration Error | Credenciales AWS no configuradas en el servidor | Contactar al administrador. |
| 500 | Internal Error | Error inesperado del servidor | Reportar al administrador con el `job_id` si es posible. |

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
  |<───────            | RUNNING|        ├──> GET /jobs/:id/od/:poiId          (por POI)
  |                    | (auto- |        └──> GET /jobs/:name/analysis         (metricas diarias)
  |  webhook?          |  check)|
  |  ─ ─ ─ ─ ─ ─ ─ ─> |        |
  |                    v        |
  |               SUCCESS ──────┘
  |               (auto-sync)
```

### Paso a Paso

1. **`POST /api/external/jobs`** — Crear job, recibir `job_id`
2. **`GET /api/external/jobs/:jobId/status`** — Consultar periodicamente (cada 5 min) hasta que `status = "SUCCESS"`. Alternativamente, registrar un `webhook_url` y esperar la notificacion.
3. Cuando `status = "SUCCESS"` y `synced = true`:
   - Los **resultados basicos** (pings, dispositivos, resumen por POI) ya estan en la respuesta de status.
   - **`GET /api/external/jobs/:jobId/catchment`** — Origenes residenciales de todos los POIs.
   - **`GET /api/external/jobs/:jobId/catchment/:poiId`** — Origenes residenciales de un POI especifico.
   - **`GET /api/external/jobs/:jobId/od`** — Origen-destino de todos los POIs.
   - **`GET /api/external/jobs/:jobId/od/:poiId`** — Origen-destino de un POI especifico.
   - **`GET /api/external/jobs/:datasetName/analysis`** — Metricas diarias y POIs activos.

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
| Analisis diario (primera peticion) | 30-90 segundos |
| Analisis diario cacheado | < 1 segundo |

### Intervalo de Polling Recomendado

**Cada 5 minutos.** No hay beneficio en consultar mas frecuentemente ya que los jobs tardan horas. El endpoint de status verifica automaticamente con Veraset en cada llamada, asi que siempre obtienes el estado mas reciente.
