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

## Mega-Jobs (Extended Date Ranges)

Mega-jobs allow you to run mobility analyses across date ranges exceeding 31 days. The system automatically splits the date range into sub-jobs of 31 days each and provides consolidated reporting across all sub-jobs.

### 5. Upload POI Collection

**`POST /api/pois/collections`**

Uploads a GeoJSON FeatureCollection for use with mega-jobs. This must be done before creating a mega-job.

#### Request Body

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | No | Collection identifier. Auto-generated from `name` if omitted. |
| `name` | string | Yes | Human-readable collection name |
| `poiCount` | number | Yes | Number of POIs in the collection |
| `geojson` | object | Yes | Valid GeoJSON FeatureCollection |

#### Example Request

```bash
curl -X POST https://your-domain.com/api/pois/collections \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "my-collection-id",
    "name": "Spain Tobacco POIs",
    "poiCount": 150,
    "geojson": {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "geometry": { "type": "Point", "coordinates": [-3.7038, 40.4168] },
          "properties": { "id": "poi_001", "name": "Estanco Sol" }
        }
      ]
    }
  }'
```

#### Response (200)

```json
{
  "id": "my-collection-id",
  "name": "Spain Tobacco POIs",
  "poiCount": 150,
  "totalFeatures": 150,
  "invalidFeatures": 0,
  "geojsonPath": "pois/my-collection-id.geojson",
  "createdAt": "2026-03-20T10:00:00.000Z"
}
```

---

### 6. Create Mega-Job (Auto-Split)

**`POST /api/mega-jobs`**

Creates a mega-job that automatically splits a date range exceeding 31 days into sub-jobs. Each sub-job covers at most 31 days.

#### Request Body

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `mode` | string | Yes | Must be `"auto-split"` |
| `name` | string | Yes | Job name (1-200 characters) |
| `description` | string | No | Optional description (max 1000 characters) |
| `country` | string | No | ISO 2-letter country code (e.g., `"ES"`) |
| `poiCollectionId` | string | Yes | ID of a previously uploaded POI collection |
| `dateRange.from` | string | Yes | Start date (YYYY-MM-DD) |
| `dateRange.to` | string | Yes | End date (YYYY-MM-DD). No maximum limit. |
| `radius` | number | No | Search radius in meters (1-1000). Default: `10` |
| `schema` | string | No | `BASIC`, `ENHANCED`, or `FULL`. Default: `BASIC` |
| `type` | string | No | `pings`, `devices`, `aggregate`, `cohort`, or `pings_by_device`. Default: `pings` |

#### Example Request

```bash
curl -X POST https://your-domain.com/api/mega-jobs \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "auto-split",
    "name": "Spain Tobacco Q1 2026",
    "country": "ES",
    "poiCollectionId": "my-collection-id",
    "dateRange": { "from": "2026-01-01", "to": "2026-03-31" },
    "radius": 10,
    "schema": "BASIC",
    "type": "pings"
  }'
```

#### Response (201 Created)

```json
{
  "megaJob": {
    "megaJobId": "mj-abc123",
    "name": "Spain Tobacco Q1 2026",
    "status": "planning",
    "splits": {
      "dateChunks": [
        { "from": "2026-01-01", "to": "2026-01-31" },
        { "from": "2026-02-01", "to": "2026-03-03" },
        { "from": "2026-03-04", "to": "2026-03-31" }
      ],
      "poiChunks": [{ "startIndex": 0, "endIndex": 150, "count": 150 }],
      "totalSubJobs": 3
    },
    "progress": { "created": 0, "synced": 0, "failed": 0, "total": 3 }
  },
  "splitPreview": {
    "totalSubJobs": 3,
    "totalPois": 150
  }
}
```

#### Mega-Job Statuses

| Status | Description |
|--------|-------------|
| `planning` | Mega-job created, date range split calculated, sub-jobs not yet submitted |
| `creating` | Sub-jobs are being created via Veraset API |
| `running` | All sub-jobs submitted, waiting for completion |
| `consolidating` | Sub-jobs completed, reports being consolidated |
| `completed` | All reports consolidated and available |
| `partial` | Some sub-jobs failed but partial results are available |
| `error` | All sub-jobs failed or a critical error occurred |

---

### 7. Create Sub-Jobs (Poll)

**`POST /api/mega-jobs/:megaJobId/create-poll`**

Creates one sub-job per call via the Veraset API. Call this endpoint repeatedly until the response contains `"done": true`.

#### Example Request

```bash
curl -X POST https://your-domain.com/api/mega-jobs/mj-abc123/create-poll \
  -H "X-API-Key: YOUR_API_KEY"
```

#### Response - In Progress

```json
{
  "megaJob": {
    "megaJobId": "mj-abc123",
    "status": "creating",
    "progress": { "created": 1, "synced": 0, "failed": 0, "total": 3 }
  },
  "createdJobId": "veraset-job-xyz",
  "done": false
}
```

#### Response - All Sub-Jobs Created

```json
{
  "megaJob": {
    "megaJobId": "mj-abc123",
    "status": "running",
    "progress": { "created": 3, "synced": 0, "failed": 0, "total": 3 }
  },
  "done": true
}
```

> **Note:** Recommended polling interval: **every 30-60 seconds**.

---

### 8. Get Mega-Job Detail

**`GET /api/mega-jobs/:megaJobId`**

Returns full mega-job detail including all sub-job statuses.

#### Example Request

```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  https://your-domain.com/api/mega-jobs/mj-abc123
```

#### Response

```json
{
  "megaJobId": "mj-abc123",
  "name": "Spain Tobacco Q1 2026",
  "status": "running",
  "progress": { "created": 3, "synced": 2, "failed": 0, "total": 3 },
  "subJobs": [
    {
      "jobId": "xyz1",
      "name": "Spain Tobacco Q1 2026 [2026-01-01→2026-01-31]",
      "status": "SUCCESS",
      "syncedAt": "2026-03-20T14:00:00.000Z"
    },
    {
      "jobId": "xyz2",
      "name": "Spain Tobacco Q1 2026 [2026-02-01→2026-03-03]",
      "status": "SUCCESS",
      "syncedAt": "2026-03-20T14:10:00.000Z"
    },
    {
      "jobId": "xyz3",
      "name": "Spain Tobacco Q1 2026 [2026-03-04→2026-03-31]",
      "status": "RUNNING"
    }
  ]
}
```

> **Note:** Recommended polling interval: **every 5 minutes** (sub-jobs typically take 1-4 hours each).

---

### 9. Consolidate Reports

**`POST /api/mega-jobs/:megaJobId/consolidate`**

Runs multi-phase consolidation with parallel Athena queries across all completed sub-jobs. This endpoint is idempotent — call it repeatedly until the phase reaches `"done"`.

#### Request Body (Optional)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `poiIds` | array | No | Filter consolidation to specific POI IDs |

#### Example Request

```bash
curl -X POST https://your-domain.com/api/mega-jobs/mj-abc123/consolidate \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{ "poiIds": ["poi_001", "poi_002"] }'
```

#### Consolidation Phases

| Phase | Description |
|-------|-------------|
| `starting` | Launches 7 parallel Athena queries (visits, OD, hourly, catchment, mobility, temporal, MAIDs) |
| `polling` | Polls Athena queries until all complete |
| `parsing_visits` | Parses visits, hourly, and mobility results |
| `parsing_od` | Parses origin-destination and catchment results with geocoding |
| `done` | All reports saved and available for retrieval |

#### Response - In Progress

```json
{
  "phase": "polling",
  "progress": {
    "step": "polling_queries",
    "percent": 25,
    "message": "Queries: 3 done, 4 running..."
  }
}
```

#### Response - Complete

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

> **Note:** Recommended polling interval: **every 30-60 seconds**.

---

### 10. Get Consolidated Reports

**`GET /api/mega-jobs/:megaJobId/reports?type={type}`**

Returns a consolidated JSON report after consolidation is complete.

#### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `type` | string | Yes | Report type to retrieve |

#### Valid Report Types

| Type | Description |
|------|-------------|
| `visits` | Visit counts per POI across the full date range |
| `temporal` | Temporal patterns (day-of-week, time-of-day distributions) |
| `catchment` | Residential zipcode distribution of visitors |
| `od` | Origin-destination matrix with geocoded locations |
| `hourly` | Hourly visit breakdown per POI |
| `mobility` | Mobility flow patterns between POIs |

#### Example Request

```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  https://your-domain.com/api/mega-jobs/mj-abc123/reports?type=visits
```

---

### 11. Download Consolidated Reports (CSV)

**`GET /api/mega-jobs/:megaJobId/reports/download?type={type}`**

Downloads a consolidated report as a CSV file.

#### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `type` | string | Yes | Report type to download |

#### Valid Download Types

`visits`, `temporal`, `catchment`, `od`, `hourly`, `mobility`, `maids`, `postcodes`

> **Note:** The `maids` and `postcodes` types are only available as CSV downloads and are not available via the JSON reports endpoint.

#### Example Request

```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  -o temporal-report.csv \
  https://your-domain.com/api/mega-jobs/mj-abc123/reports/download?type=temporal
```

---

## Error Codes

| HTTP Status | Error | Description |
|-------------|-------|-------------|
| 400 | Validation Error | Request body failed validation. Check the error message. |
| 401 | Unauthorized | Missing or invalid API key |
| 404 | Not Found | Job ID, mega-job ID, or POI collection not found |
| 409 | Job Not Ready | Job not completed yet (for catchment or consolidation requests) |
| 409 | Consolidation In Progress | A consolidation is already running for this mega-job |
| 422 | Invalid GeoJSON | Uploaded GeoJSON is malformed or contains no valid features |
| 422 | Collection Not Found | The referenced `poiCollectionId` does not exist |
| 429 | Rate Limited | Too many requests. Check `Retry-After` header. |
| 502 | Upstream Error | Veraset API call failed |
| 500 | Internal Error | Server error |

---

## Typical Workflow

### Standard Jobs (up to 31 days)

```
1. POST /api/external/jobs             --> Create job, get job_id
2. GET  /api/external/jobs/:id/status   --> Poll until status = SUCCESS
   (or wait for webhook notification)
3. GET  /api/external/jobs/:id/status   --> Get results (pings, devices, per-POI)
4. GET  /api/external/jobs/:id/catchment --> Get residential zipcode matrix
```

Recommended polling interval: **every 5 minutes** (jobs typically take 1-4 hours).

### Mega-Jobs (date ranges exceeding 31 days)

```
1. POST /api/pois/collections               --> Upload POI collection
2. POST /api/mega-jobs                       --> Create mega-job (auto-split)
3. POST /api/mega-jobs/:id/create-poll       --> Loop until done: true (creates 1 sub-job per call)
4. GET  /api/mega-jobs/:id                   --> Poll until all sub-jobs reach SUCCESS
5. POST /api/mega-jobs/:id/consolidate       --> Loop until phase = "done"
6. GET  /api/mega-jobs/:id/reports?type=visits    --> Get consolidated JSON report
7. GET  /api/mega-jobs/:id/reports/download?type=temporal --> Download CSV report
```

Recommended polling intervals:
- **create-poll**: every 30-60 seconds
- **mega-job detail** (sub-job progress): every 5 minutes
- **consolidation**: every 30-60 seconds
