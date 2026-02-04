# Veraset API Management Platform

A Next.js web application for managing Veraset geospatial mobility data workflows, including POI management, API job creation, S3 data synchronization, and audience analysis.

## Features

- **Dashboard**: API usage tracking, active jobs monitoring, and quick stats
- **POI Management**: Upload, import from OSM/Overture, deduplication, and collection management
- **Job Creator**: Create and configure Veraset API jobs with validation
- **Job Monitor**: Track job status with real-time polling
- **S3 Sync Manager**: Sync Veraset output to GMC S3 buckets
- **Data Analysis**: Analyze synced datasets with dwell time and activity metrics
- **Audience Export**: Export device audiences in various formats (CSV, JSON, DSP-ready)

## Tech Stack

- **Frontend**: Next.js 14 (App Router), TypeScript, Tailwind CSS, shadcn/ui
- **Backend**: Next.js API routes
- **Database**: PostgreSQL (Supabase)
- **Storage**: AWS S3
- **Charts**: Recharts

## Prerequisites

- Node.js 18+ 
- AWS account with S3 access
- Veraset API key

## Setup

1. **Install dependencies**:
   ```bash
   npm install
   ```

2. **Set up environment variables**:
   Copy `.env.example` to `.env.local` and fill in your credentials:
   ```bash
   cp .env.example .env.local
   ```

   Required variables:
   - `VERASET_API_KEY`: Your Veraset API key
   - `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`: AWS credentials for S3
   - `S3_BUCKET`: S3 bucket name (default: `garritz-veraset-data-us-west-2`)

3. **S3 Bucket Structure**:
   The app will automatically create the following structure in your S3 bucket:
   ```
   s3://garritz-veraset-data-us-west-2/
   ├── config/
   │   ├── usage.json              # Monthly API usage tracking
   │   ├── jobs.json               # All job metadata
   │   └── poi-collections.json    # Saved POI collection index
   ├── pois/
   │   └── {collection-id}.geojson # Full POI data per collection
   └── {dataset-name}/             # Veraset data (partitioned parquet)
   ```

4. **Run development server**:
   ```bash
   npm run dev
   ```

   Open [http://localhost:3000](http://localhost:3000) in your browser.

## Deployment

### Vercel

1. Push your code to GitHub
2. Import the repository in Vercel
3. Add environment variables in Vercel dashboard
4. Deploy

The existing API routes in `/api/veraset/*` will continue to work alongside the new Next.js app.

## Project Structure

```
/app
  /dashboard          - Dashboard page
  /pois               - POI management
  /jobs               - Job management
  /datasets           - Synced datasets
  /export             - Audience export
  /api                - API routes
/components
  /ui                 - shadcn/ui components
  /jobs               - Job-specific components
/lib
  veraset-client.ts   - Veraset API client
  s3.ts               - S3 operations
  geo.ts              - Geo utilities (Haversine, GeoJSON)
  supabase.ts         - Database client
```

## API Endpoints

### Existing (Vercel Serverless)
- `GET /api/health` - Health check
- `POST /api/veraset/movement` - Create movement job
- `GET /api/veraset/job/[id]` - Check job status
- `GET /api/veraset/categories` - List POI categories
- `POST /api/veraset/pois` - Query Veraset POI database

### New (Next.js API Routes)
- `GET /api/jobs` - List all jobs
- `POST /api/jobs` - Create job
- `GET /api/jobs/[id]` - Get job details
- `POST /api/jobs/[id]/sync` - Trigger S3 sync
- `GET /api/pois/collections` - List POI collections
- `POST /api/pois/collections` - Save POI collection
- `GET /api/datasets` - List synced datasets
- `POST /api/export` - Generate audience export

## Data Storage

All data is stored in S3 as JSON files:

- **config/jobs.json**: All job metadata indexed by job ID
- **config/poi-collections.json**: POI collection metadata indexed by collection ID
- **config/usage.json**: Monthly API usage tracking
- **pois/{collection-id}.geojson**: Full GeoJSON data for each POI collection

## Development Notes

- All data is stored in S3 JSON files - no database required
- Job status polling happens every 30 seconds for QUEUED/RUNNING jobs
- S3 sync operations copy objects from Veraset bucket to GMC bucket
- POI deduplication uses Haversine formula with configurable radius (default 50m)
- Config files are automatically created on first write

## License

Private - GMC Internal Use
