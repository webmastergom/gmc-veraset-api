# Garritz Marketing Cloud Mobility API - Documentación de API Externa

## Resumen

La API Externa proporciona acceso programático a los resultados de análisis de datasets de movilidad de Veraset. Esta API permite a clientes externos:

- Listar datasets/jobs disponibles
- Obtener métricas diarias (pings y dispositivos únicos por día)
- Obtener POIs activos (Puntos de Interés) con coordenadas y nombres

## URL Base

```
Producción: https://your-domain.com/api/external
Desarrollo: http://localhost:3000/api/external
```

## Autenticación

Todos los endpoints de la API requieren autenticación usando una clave API. Puedes proporcionar la clave API de dos formas:

### Opción 1: Header HTTP (Recomendado)
```http
X-API-Key: tu-clave-api-aqui
```

### Opción 2: Parámetro de Consulta
```
?api_key=tu-clave-api-aqui
```

**Nota de Seguridad**: Se recomienda el método de header ya que previene que las claves API aparezcan en los logs del servidor y en las URLs.

### Obtener una Clave API

Contacta a tu administrador para obtener una clave API. La clave se proporcionará una vez y debe almacenarse de forma segura. Si pierdes tu clave API, necesitarás solicitar una nueva.

## Endpoints

### 1. Listar Datasets Disponibles

Obtiene una lista de todos los datasets disponibles que se han completado exitosamente.

**Endpoint**: `GET /api/external/jobs`

**Autenticación**: Requerida

**Respuesta**:
```json
{
  "jobs": [
    {
      "datasetName": "spain-nicotine-full-jan",
      "jobName": "Spain Nicotine Full - January",
      "jobId": "5bb5f893-56c0-4f9f-a3b2-e0e3df7c7fb9",
      "dateRange": {
        "from": "2026-01-01",
        "to": "2026-01-31"
      },
      "status": "SUCCESS",
      "poiCount": 2200
    }
  ]
}
```

**Campos de Respuesta**:
- `jobs` (array): Lista de datasets disponibles
  - `datasetName` (string): Identificador único del dataset (úsalo para solicitudes de análisis)
  - `jobName` (string): Nombre legible del job
  - `jobId` (string): Identificador interno del job
  - `dateRange` (object): Rango de fechas cubierto por el dataset
    - `from` (string): Fecha de inicio (YYYY-MM-DD)
    - `to` (string): Fecha de fin (YYYY-MM-DD)
  - `status` (string): Estado del job (siempre "SUCCESS" para datasets disponibles)
  - `poiCount` (number): Número de POIs en el dataset

**Ejemplo de Solicitud**:
```bash
curl -X GET "https://your-domain.com/api/external/jobs" \
  -H "X-API-Key: tu-clave-api-aqui"
```

---

### 2. Obtener Análisis del Dataset

Obtiene datos de análisis detallados para un dataset específico, incluyendo métricas diarias y POIs activos.

**Endpoint**: `GET /api/external/jobs/{datasetName}/analysis`

**Autenticación**: Requerida

**Parámetros de Ruta**:
- `datasetName` (string, requerido): El nombre del dataset del endpoint de lista de jobs

**Respuesta**:
```json
{
  "datasetName": "spain-nicotine-full-jan",
  "jobName": "Spain Nicotine Full - January",
  "analysis": {
    "dailyPings": [
      {
        "date": "2026-01-01",
        "pings": 125000
      },
      {
        "date": "2026-01-02",
        "pings": 132000
      }
    ],
    "dailyDevices": [
      {
        "date": "2026-01-01",
        "devices": 45000
      },
      {
        "date": "2026-01-02",
        "devices": 47000
      }
    ],
    "pois": [
      {
        "poiId": "geo_radius_0",
        "name": "Tobacco Shop Madrid",
        "latitude": 40.4168,
        "longitude": -3.7038
      },
      {
        "poiId": "geo_radius_1",
        "name": "Cigarette Store Barcelona",
        "latitude": 41.3851,
        "longitude": 2.1734
      }
    ]
  }
}
```

**Campos de Respuesta**:
- `datasetName` (string): El identificador del dataset
- `jobName` (string): Nombre legible del job
- `analysis` (object): Resultados del análisis
  - `dailyPings` (array): Conteos diarios de pings
    - `date` (string): Fecha en formato YYYY-MM-DD
    - `pings` (number): Total de pings para ese día
  - `dailyDevices` (array): Conteos diarios de dispositivos únicos
    - `date` (string): Fecha en formato YYYY-MM-DD
    - `devices` (number): Número de dispositivos únicos para ese día
  - `pois` (array): POIs activos (solo POIs con actividad en el dataset)
    - `poiId` (string): Identificador del POI (puede ser generado por Veraset como `geo_radius_X`)
    - `name` (string): Nombre legible del POI (si está disponible)
    - `latitude` (number): Coordenada de latitud del POI
    - `longitude` (number): Coordenada de longitud del POI

**Nota**: Los arrays `dailyPings` y `dailyDevices` están alineados por fecha - cada fecha aparece en ambos arrays con las métricas correspondientes.

**Ejemplo de Solicitud**:
```bash
curl -X GET "https://your-domain.com/api/external/jobs/spain-nicotine-full-jan/analysis" \
  -H "X-API-Key: tu-clave-api-aqui"
```

---

## Respuestas de Error

Todos los endpoints devuelven códigos de estado HTTP estándar. Las respuestas de error siguen este formato:

```json
{
  "error": "Tipo de error",
  "message": "Mensaje de error legible",
  "details": "Detalles adicionales del error (opcional)"
}
```

### Códigos de Estado

- `200 OK`: Solicitud exitosa
- `400 Bad Request`: Parámetros de solicitud inválidos
- `401 Unauthorized`: Clave API faltante o inválida
- `404 Not Found`: Dataset no encontrado o no existe
- `500 Internal Server Error`: Ocurrió un error del servidor
- `503 Service Unavailable`: Servicio temporalmente no disponible (ej., credenciales de AWS no configuradas)

### Ejemplos de Errores Comunes

**401 Unauthorized** (Clave API Faltante):
```json
{
  "error": "Unauthorized",
  "message": "API key is required. Provide it via X-API-Key header or api_key query parameter."
}
```

**401 Unauthorized** (Clave API Inválida):
```json
{
  "error": "Unauthorized",
  "message": "Invalid API key. Please check your API key and try again."
}
```

**404 Not Found** (Dataset no existe):
```json
{
  "error": "Not Found",
  "message": "Dataset 'invalid-dataset-name' not found or not available."
}
```

**500 Internal Server Error** (Análisis falló):
```json
{
  "error": "Internal Server Error",
  "message": "Failed to analyze dataset",
  "details": "Analysis query failed: [detalles específicos del error]"
}
```

---

## Ejemplos de Código

### cURL

**Listar Datasets**:
```bash
curl -X GET "https://your-domain.com/api/external/jobs" \
  -H "X-API-Key: tu-clave-api-aqui" \
  -H "Content-Type: application/json"
```

**Obtener Análisis**:
```bash
curl -X GET "https://your-domain.com/api/external/jobs/spain-nicotine-full-jan/analysis" \
  -H "X-API-Key: tu-clave-api-aqui" \
  -H "Content-Type: application/json"
```

### Python

```python
import requests

API_BASE_URL = "https://your-domain.com/api/external"
API_KEY = "tu-clave-api-aqui"

headers = {
    "X-API-Key": API_KEY,
    "Content-Type": "application/json"
}

# Listar datasets disponibles
response = requests.get(f"{API_BASE_URL}/jobs", headers=headers)
if response.status_code == 200:
    data = response.json()
    jobs = data["jobs"]
    print(f"Se encontraron {len(jobs)} datasets")
    for job in jobs:
        print(f"- {job['jobName']} ({job['datasetName']})")
else:
    print(f"Error: {response.status_code} - {response.text}")

# Obtener análisis para un dataset específico
dataset_name = "spain-nicotine-full-jan"
response = requests.get(
    f"{API_BASE_URL}/jobs/{dataset_name}/analysis",
    headers=headers
)

if response.status_code == 200:
    data = response.json()
    analysis = data["analysis"]
    
    # Procesar pings diarios
    for day in analysis["dailyPings"]:
        print(f"{day['date']}: {day['pings']:,} pings")
    
    # Procesar dispositivos diarios
    for day in analysis["dailyDevices"]:
        print(f"{day['date']}: {day['devices']:,} dispositivos únicos")
    
    # Procesar POIs
    print(f"\nSe encontraron {len(analysis['pois'])} POIs activos:")
    for poi in analysis["pois"]:
        print(f"- {poi['name']} ({poi['latitude']}, {poi['longitude']})")
else:
    print(f"Error: {response.status_code} - {response.text}")
```

### JavaScript/Node.js

```javascript
const API_BASE_URL = 'https://your-domain.com/api/external';
const API_KEY = 'tu-clave-api-aqui';

const headers = {
  'X-API-Key': API_KEY,
  'Content-Type': 'application/json'
};

// Listar datasets disponibles
async function listDatasets() {
  try {
    const response = await fetch(`${API_BASE_URL}/jobs`, { headers });
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const data = await response.json();
    console.log(`Se encontraron ${data.jobs.length} datasets`);
    
    data.jobs.forEach(job => {
      console.log(`- ${job.jobName} (${job.datasetName})`);
    });
    
    return data.jobs;
  } catch (error) {
    console.error('Error al listar datasets:', error);
    throw error;
  }
}

// Obtener análisis para un dataset específico
async function getAnalysis(datasetName) {
  try {
    const response = await fetch(
      `${API_BASE_URL}/jobs/${datasetName}/analysis`,
      { headers }
    );
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.message || `HTTP error! status: ${response.status}`);
    }
    
    const data = await response.json();
    const { analysis } = data;
    
    // Procesar pings diarios
    console.log('\nPings Diarios:');
    analysis.dailyPings.forEach(day => {
      console.log(`${day.date}: ${day.pings.toLocaleString()} pings`);
    });
    
    // Procesar dispositivos únicos diarios
    console.log('\nDispositivos Únicos Diarios:');
    analysis.dailyDevices.forEach(day => {
      console.log(`${day.date}: ${day.devices.toLocaleString()} dispositivos`);
    });
    
    // Procesar POIs
    console.log(`\nSe encontraron ${analysis.pois.length} POIs activos:`);
    analysis.pois.forEach(poi => {
      console.log(`- ${poi.name} (${poi.latitude}, ${poi.longitude})`);
    });
    
    return data;
  } catch (error) {
    console.error('Error al obtener análisis:', error);
    throw error;
  }
}

// Ejemplo de uso
(async () => {
  try {
    const jobs = await listDatasets();
    if (jobs.length > 0) {
      const firstDataset = jobs[0].datasetName;
      await getAnalysis(firstDataset);
    }
  } catch (error) {
    console.error('Error:', error);
  }
})();
```

### PHP

```php
<?php
$apiBaseUrl = 'https://your-domain.com/api/external';
$apiKey = 'tu-clave-api-aqui';

$headers = [
    'X-API-Key: ' . $apiKey,
    'Content-Type: application/json'
];

// Listar datasets disponibles
function listDatasets($apiBaseUrl, $headers) {
    $ch = curl_init($apiBaseUrl . '/jobs');
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
    
    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    
    if ($httpCode === 200) {
        $data = json_decode($response, true);
        echo "Se encontraron " . count($data['jobs']) . " datasets\n";
        foreach ($data['jobs'] as $job) {
            echo "- {$job['jobName']} ({$job['datasetName']})\n";
        }
        return $data['jobs'];
    } else {
        echo "Error: HTTP $httpCode\n";
        return null;
    }
}

// Obtener análisis para un dataset específico
function getAnalysis($apiBaseUrl, $headers, $datasetName) {
    $ch = curl_init($apiBaseUrl . '/jobs/' . $datasetName . '/analysis');
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
    
    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    
    if ($httpCode === 200) {
        $data = json_decode($response, true);
        $analysis = $data['analysis'];
        
        echo "\nPings Diarios:\n";
        foreach ($analysis['dailyPings'] as $day) {
            echo "{$day['date']}: " . number_format($day['pings']) . " pings\n";
        }
        
        echo "\nDispositivos Únicos Diarios:\n";
        foreach ($analysis['dailyDevices'] as $day) {
            echo "{$day['date']}: " . number_format($day['devices']) . " dispositivos\n";
        }
        
        echo "\nSe encontraron " . count($analysis['pois']) . " POIs activos:\n";
        foreach ($analysis['pois'] as $poi) {
            echo "- {$poi['name']} ({$poi['latitude']}, {$poi['longitude']})\n";
        }
        
        return $data;
    } else {
        $error = json_decode($response, true);
        echo "Error: HTTP $httpCode - " . ($error['message'] ?? 'Error desconocido') . "\n";
        return null;
    }
}

// Ejemplo de uso
$jobs = listDatasets($apiBaseUrl, $headers);
if ($jobs && count($jobs) > 0) {
    $firstDataset = $jobs[0]['datasetName'];
    getAnalysis($apiBaseUrl, $headers, $firstDataset);
}
?>
```

---

## Límites de Tasa (Rate Limiting)

Actualmente, no hay límites de tasa aplicados. Sin embargo, por favor usa la API de manera responsable:

- Evita hacer solicitudes excesivas en períodos cortos de tiempo
- Almacena en caché los resultados cuando sea apropiado
- Contacta al soporte si necesitas límites de tasa más altos

Los límites de tasa pueden implementarse en el futuro y se comunicarán con anticipación.

---

## Mejores Prácticas

1. **Almacena las Claves API de Forma Segura**: Nunca comprometas las claves API al control de versiones ni las expongas en código del lado del cliente.

2. **Usa HTTPS**: Siempre usa HTTPS en producción para cifrar la transmisión de la clave API.

3. **Maneja Errores con Elegancia**: Implementa manejo adecuado de errores para todos los códigos de estado HTTP.

4. **Almacena Resultados en Caché**: Los resultados del análisis de datasets no cambian frecuentemente. Considera almacenar resultados en caché para reducir las llamadas a la API.

5. **Usa Headers Apropiados**: Prefiere el header `X-API-Key` sobre los parámetros de consulta para mejor seguridad.

6. **Monitorea el Uso**: Mantén un registro de tu uso de la API para identificar cualquier problema o patrón inesperado.

---

## Soporte

Para soporte de la API, preguntas o para reportar problemas:

- Contacta a tu administrador para la gestión de claves API
- Reporta bugs o problemas a través de tu canal de soporte designado
- Revisa la página de estado de la API (si está disponible)

---

## Historial de Cambios

### Versión 1.0.0 (Lanzamiento Inicial)
- Endpoint para listar datasets disponibles
- Endpoint para obtener análisis de dataset con métricas diarias y POIs
- Autenticación con clave API

---

## Licencia

Esta documentación de la API se proporciona solo para usuarios autorizados. El acceso o uso no autorizado está prohibido.
