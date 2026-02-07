# Mobility API - External API Reference

## Authentication

All API requests require an API key. Include it in the `X-API-Key` header:

```bash
curl -H "X-API-Key: YOUR_API_KEY" https://your-domain.com/api/external/jobs
```

Alternatively, pass it as a query parameter: `?api_key=YOUR_API_KEY`

## Rate Limits

- **100 requests per minute** per API key
- Rate limit headers are included in every response:
  - `X-RateLimit-Limit`: Maximum requests per window
  - `X-RateLimit-Remaining`: Remaining requests
  - `X-RateLimit-Reset`: Unix timestamp when the limit resets

---

## Endpoints

### 1. Create Job

**`POST /api/external/jobs`**

Submit a mobility analysis job by providing POIs, date range, and configuration.

#### Request Body

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Job name (1-200 characters) |
| `country` | string | Yes | ISO 2-letter country code (e.g., "ES") |
| `type` | string | No | Job type: `pings`, `devices`, or `aggregate`. Default: `pings` |
| `date_range.from` | string | Yes | Start date (YYYY-MM-DD) |
| `date_range.to` | string | Yes | End date (YYYY-MM-DD). Max 31 days from start. |
| `radius` | number | No | Search radius in meters (1-1000). Default: 10 |
| `schema` | string | No | Data schema: `BASIC`, `ENHANCED`, or `FULL`. Default: `BASIC` |
| `pois` | array | Yes | Array of POI objects (1-25,000 items) |
| `pois[].id` | string | Yes | Unique POI identifier |
| `pois[].name` | string | No | Human-readable POI name |
| `pois[].latitude` | number | Yes | Latitude (-90 to 90) |
| `pois[].longitude` | number | Yes | Longitude (-180 to 180) |
| `webhook_url` | string | No | HTTPS URL for status change notifications |

#### Example Request

```bash
curl -X POST https://your-domain.com/api/external/jobs \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Spain Tobacco Stores - January 2026",
    "country": "ES",
    "type": "pings",
    "date_range": { "from": "2026-01-01", "to": "2026-01-31" },
    "radius": 10,
    "schema": "BASIC",
    "pois": [
      { "id": "estanco_001", "name": "Estanco Sol", "latitude": 40.4168, "longitude": -3.7038 },
      { "id": "estanco_002", "name": "Estanco Gran Via", "latitude": 40.4200, "longitude": -3.7050 }
    ],
    "webhook_url": "https://your-server.com/webhooks/mobility"
  }'
```

#### Response (201 Created)

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

### 2. Get Job Status

**`GET /api/external/jobs/:jobId/status`**

Check the current status of a job. If the job is still processing, this endpoint will automatically check the upstream provider for status updates.

#### Example Request

```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  https://your-domain.com/api/external/jobs/abc123xyz/status
```

#### Response - Processing

```json
{
  "job_id": "abc123xyz",
  "status": "RUNNING",
  "created_at": "2026-02-07T10:00:00.000Z",
  "updated_at": "2026-02-07T10:05:00.000Z"
}
```

#### Response - Completed

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

#### Response - Failed

```json
{
  "job_id": "abc123xyz",
  "status": "FAILED",
  "created_at": "2026-02-07T10:00:00.000Z",
  "error": "Processing error description"
}
```

#### Job Statuses

| Status | Description |
|--------|-------------|
| `QUEUED` | Job submitted, waiting to be processed |
| `RUNNING` | Job is being processed |
| `SUCCESS` | Job completed successfully, results available |
| `FAILED` | Job failed, check `error` field |

---

### 3. Get Catchment Analysis

**`GET /api/external/jobs/:jobId/catchment`**

Get the residential zipcode distribution of visitors who passed through the POIs. This analysis determines where visitors live based on nighttime location patterns.

Only available for jobs with status `SUCCESS`.

#### Example Request

```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  https://your-domain.com/api/external/jobs/abc123xyz/catchment
```

#### Response

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
      "region": "Cataluña",
      "devices": 987,
      "percentage": 3.46
    }
  ]
}
```

> **Note:** For privacy, only zipcodes with 5 or more devices are included.

> **Note:** The first request may take 1-3 minutes as it runs Athena queries and reverse geocoding. Results are cached for subsequent requests.

---

### 4. List Available Jobs

**`GET /api/external/jobs`**

List all completed jobs with available datasets.

#### Example Request

```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  https://your-domain.com/api/external/jobs
```

#### Response

```json
{
  "jobs": [
    {
      "datasetName": "spain-tobacco-jan-2026",
      "jobName": "Spain Tobacco Stores - January 2026",
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

If you provide a `webhook_url` when creating a job, you will receive HTTP POST notifications when the job status changes.

### Webhook Payload

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

### Webhook Requirements

- URL must use HTTPS
- Your endpoint must respond with 2xx within 10 seconds
- Failed webhooks are retried once after 2 seconds
- Webhook requests include `Content-Type: application/json`

---

## Error Codes

| HTTP Status | Error | Description |
|-------------|-------|-------------|
| 400 | Validation Error | Request body failed validation. Check the error message. |
| 401 | Unauthorized | Missing or invalid API key |
| 404 | Not Found | Job ID not found |
| 409 | Job Not Ready | Job not completed yet (for catchment requests) |
| 429 | Rate Limited | Too many requests. Check `Retry-After` header. |
| 502 | Upstream Error | Veraset API call failed |
| 500 | Internal Error | Server error |

---

## Typical Workflow

```
1. POST /api/external/jobs          --> Create job, get job_id
2. GET  /api/external/jobs/:id/status  --> Poll until status = SUCCESS
   (or wait for webhook notification)
3. GET  /api/external/jobs/:id/status  --> Get results (pings, devices, per-POI)
4. GET  /api/external/jobs/:id/catchment --> Get residential zipcode matrix
```

Recommended polling interval: **every 5 minutes** (jobs typically take 1-4 hours).
