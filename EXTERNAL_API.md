# Garritz Marketing Cloud Mobility API - External API Documentation

## Overview

The External API provides programmatic access to Veraset mobility dataset analysis results. This API allows external clients to:

- List available datasets/jobs
- Retrieve daily metrics (pings and unique devices per day)
- Get active POIs (Points of Interest) with coordinates and names

## Base URL

```
Production: https://your-domain.com/api/external
Development: http://localhost:3000/api/external
```

## Authentication

All API endpoints require authentication using an API key. You can provide the API key in one of two ways:

### Option 1: HTTP Header (Recommended)
```http
X-API-Key: your-api-key-here
```

### Option 2: Query Parameter
```
?api_key=your-api-key-here
```

**Security Note**: The header method is recommended as it prevents API keys from appearing in server logs and URLs.

### Obtaining an API Key

Contact your administrator to obtain an API key. The key will be provided once and should be stored securely. If you lose your API key, you'll need to request a new one.

## Endpoints

### 1. List Available Datasets

Get a list of all available datasets that have completed successfully.

**Endpoint**: `GET /api/external/jobs`

**Authentication**: Required

**Response**:
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

**Response Fields**:
- `jobs` (array): List of available datasets
  - `datasetName` (string): Unique identifier for the dataset (use this for analysis requests)
  - `jobName` (string): Human-readable name of the job
  - `jobId` (string): Internal job identifier
  - `dateRange` (object): Date range covered by the dataset
    - `from` (string): Start date (YYYY-MM-DD)
    - `to` (string): End date (YYYY-MM-DD)
  - `status` (string): Job status (always "SUCCESS" for available datasets)
  - `poiCount` (number): Number of POIs in the dataset

**Example Request**:
```bash
curl -X GET "https://your-domain.com/api/external/jobs" \
  -H "X-API-Key: your-api-key-here"
```

---

### 2. Get Dataset Analysis

Retrieve detailed analysis data for a specific dataset, including daily metrics and active POIs.

**Endpoint**: `GET /api/external/jobs/{datasetName}/analysis`

**Authentication**: Required

**Path Parameters**:
- `datasetName` (string, required): The dataset name from the jobs list endpoint

**Response**:
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

**Response Fields**:
- `datasetName` (string): The dataset identifier
- `jobName` (string): Human-readable job name
- `analysis` (object): Analysis results
  - `dailyPings` (array): Daily ping counts
    - `date` (string): Date in YYYY-MM-DD format
    - `pings` (number): Total pings for that day
  - `dailyDevices` (array): Daily unique device counts
    - `date` (string): Date in YYYY-MM-DD format
    - `devices` (number): Number of unique devices for that day
  - `pois` (array): Active POIs (only POIs with activity in the dataset)
    - `poiId` (string): POI identifier (may be Veraset-generated like `geo_radius_X`)
    - `name` (string): Human-readable POI name (if available)
    - `latitude` (number): POI latitude coordinate
    - `longitude` (number): POI longitude coordinate

**Note**: The `dailyPings` and `dailyDevices` arrays are aligned by date - each date appears in both arrays with corresponding metrics.

**Example Request**:
```bash
curl -X GET "https://your-domain.com/api/external/jobs/spain-nicotine-full-jan/analysis" \
  -H "X-API-Key: your-api-key-here"
```

---

## Error Responses

All endpoints return standard HTTP status codes. Error responses follow this format:

```json
{
  "error": "Error type",
  "message": "Human-readable error message",
  "details": "Additional error details (optional)"
}
```

### Status Codes

- `200 OK`: Request successful
- `400 Bad Request`: Invalid request parameters
- `401 Unauthorized`: Missing or invalid API key
- `404 Not Found`: Dataset not found or doesn't exist
- `500 Internal Server Error`: Server error occurred
- `503 Service Unavailable`: Service temporarily unavailable (e.g., AWS credentials not configured)

### Common Error Examples

**401 Unauthorized** (Missing API Key):
```json
{
  "error": "Unauthorized",
  "message": "API key is required. Provide it via X-API-Key header or api_key query parameter."
}
```

**401 Unauthorized** (Invalid API Key):
```json
{
  "error": "Unauthorized",
  "message": "Invalid API key. Please check your API key and try again."
}
```

**404 Not Found** (Dataset doesn't exist):
```json
{
  "error": "Not Found",
  "message": "Dataset 'invalid-dataset-name' not found or not available."
}
```

**500 Internal Server Error** (Analysis failed):
```json
{
  "error": "Internal Server Error",
  "message": "Failed to analyze dataset",
  "details": "Analysis query failed: [specific error details]"
}
```

---

## Code Examples

### cURL

**List Datasets**:
```bash
curl -X GET "https://your-domain.com/api/external/jobs" \
  -H "X-API-Key: your-api-key-here" \
  -H "Content-Type: application/json"
```

**Get Analysis**:
```bash
curl -X GET "https://your-domain.com/api/external/jobs/spain-nicotine-full-jan/analysis" \
  -H "X-API-Key: your-api-key-here" \
  -H "Content-Type: application/json"
```

### Python

```python
import requests

API_BASE_URL = "https://your-domain.com/api/external"
API_KEY = "your-api-key-here"

headers = {
    "X-API-Key": API_KEY,
    "Content-Type": "application/json"
}

# List available datasets
response = requests.get(f"{API_BASE_URL}/jobs", headers=headers)
if response.status_code == 200:
    data = response.json()
    jobs = data["jobs"]
    print(f"Found {len(jobs)} datasets")
    for job in jobs:
        print(f"- {job['jobName']} ({job['datasetName']})")
else:
    print(f"Error: {response.status_code} - {response.text}")

# Get analysis for a specific dataset
dataset_name = "spain-nicotine-full-jan"
response = requests.get(
    f"{API_BASE_URL}/jobs/{dataset_name}/analysis",
    headers=headers
)

if response.status_code == 200:
    data = response.json()
    analysis = data["analysis"]
    
    # Process daily pings
    for day in analysis["dailyPings"]:
        print(f"{day['date']}: {day['pings']:,} pings")
    
    # Process daily devices
    for day in analysis["dailyDevices"]:
        print(f"{day['date']}: {day['devices']:,} unique devices")
    
    # Process POIs
    print(f"\nFound {len(analysis['pois'])} active POIs:")
    for poi in analysis["pois"]:
        print(f"- {poi['name']} ({poi['latitude']}, {poi['longitude']})")
else:
    print(f"Error: {response.status_code} - {response.text}")
```

### JavaScript/Node.js

```javascript
const API_BASE_URL = 'https://your-domain.com/api/external';
const API_KEY = 'your-api-key-here';

const headers = {
  'X-API-Key': API_KEY,
  'Content-Type': 'application/json'
};

// List available datasets
async function listDatasets() {
  try {
    const response = await fetch(`${API_BASE_URL}/jobs`, { headers });
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const data = await response.json();
    console.log(`Found ${data.jobs.length} datasets`);
    
    data.jobs.forEach(job => {
      console.log(`- ${job.jobName} (${job.datasetName})`);
    });
    
    return data.jobs;
  } catch (error) {
    console.error('Error listing datasets:', error);
    throw error;
  }
}

// Get analysis for a specific dataset
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
    
    // Process daily pings
    console.log('\nDaily Pings:');
    analysis.dailyPings.forEach(day => {
      console.log(`${day.date}: ${day.pings.toLocaleString()} pings`);
    });
    
    // Process daily devices
    console.log('\nDaily Unique Devices:');
    analysis.dailyDevices.forEach(day => {
      console.log(`${day.date}: ${day.devices.toLocaleString()} devices`);
    });
    
    // Process POIs
    console.log(`\nFound ${analysis.pois.length} active POIs:`);
    analysis.pois.forEach(poi => {
      console.log(`- ${poi.name} (${poi.latitude}, ${poi.longitude})`);
    });
    
    return data;
  } catch (error) {
    console.error('Error getting analysis:', error);
    throw error;
  }
}

// Example usage
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
$apiKey = 'your-api-key-here';

$headers = [
    'X-API-Key: ' . $apiKey,
    'Content-Type: application/json'
];

// List available datasets
function listDatasets($apiBaseUrl, $headers) {
    $ch = curl_init($apiBaseUrl . '/jobs');
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
    
    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    
    if ($httpCode === 200) {
        $data = json_decode($response, true);
        echo "Found " . count($data['jobs']) . " datasets\n";
        foreach ($data['jobs'] as $job) {
            echo "- {$job['jobName']} ({$job['datasetName']})\n";
        }
        return $data['jobs'];
    } else {
        echo "Error: HTTP $httpCode\n";
        return null;
    }
}

// Get analysis for a specific dataset
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
        
        echo "\nDaily Pings:\n";
        foreach ($analysis['dailyPings'] as $day) {
            echo "{$day['date']}: " . number_format($day['pings']) . " pings\n";
        }
        
        echo "\nDaily Unique Devices:\n";
        foreach ($analysis['dailyDevices'] as $day) {
            echo "{$day['date']}: " . number_format($day['devices']) . " devices\n";
        }
        
        echo "\nFound " . count($analysis['pois']) . " active POIs:\n";
        foreach ($analysis['pois'] as $poi) {
            echo "- {$poi['name']} ({$poi['latitude']}, {$poi['longitude']})\n";
        }
        
        return $data;
    } else {
        $error = json_decode($response, true);
        echo "Error: HTTP $httpCode - " . ($error['message'] ?? 'Unknown error') . "\n";
        return null;
    }
}

// Example usage
$jobs = listDatasets($apiBaseUrl, $headers);
if ($jobs && count($jobs) > 0) {
    $firstDataset = $jobs[0]['datasetName'];
    getAnalysis($apiBaseUrl, $headers, $firstDataset);
}
?>
```

---

## Rate Limiting

Currently, there are no rate limits enforced. However, please use the API responsibly:

- Avoid making excessive requests in short time periods
- Cache results when appropriate
- Contact support if you need higher rate limits

Rate limiting may be implemented in the future and will be communicated in advance.

---

## Best Practices

1. **Store API Keys Securely**: Never commit API keys to version control or expose them in client-side code.

2. **Use HTTPS**: Always use HTTPS in production to encrypt API key transmission.

3. **Handle Errors Gracefully**: Implement proper error handling for all HTTP status codes.

4. **Cache Results**: Dataset analysis results don't change frequently. Consider caching results to reduce API calls.

5. **Use Appropriate Headers**: Prefer the `X-API-Key` header over query parameters for better security.

6. **Monitor Usage**: Keep track of your API usage to identify any issues or unexpected patterns.

---

## Support

For API support, questions, or to report issues:

- Contact your administrator for API key management
- Report bugs or issues through your designated support channel
- Check the API status page (if available)

---

## Changelog

### Version 1.0.0 (Initial Release)
- List available datasets endpoint
- Get dataset analysis endpoint with daily metrics and POIs
- API key authentication

---

## License

This API documentation is provided for authorized users only. Unauthorized access or use is prohibited.
