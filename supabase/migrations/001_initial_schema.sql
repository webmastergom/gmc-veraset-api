-- POI Collections
CREATE TABLE IF NOT EXISTS poi_collections (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  description TEXT,
  poi_count INTEGER DEFAULT 0,
  sources JSONB DEFAULT '{}'::jsonb,
  geojson_url TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Veraset Jobs
CREATE TABLE IF NOT EXISTS veraset_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  type TEXT NOT NULL,
  poi_collection_id UUID REFERENCES poi_collections(id) ON DELETE SET NULL,
  config JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT DEFAULT 'QUEUED',
  s3_source_path TEXT,
  s3_dest_path TEXT,
  synced_at TIMESTAMPTZ,
  object_count INTEGER,
  total_bytes BIGINT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Analysis Results (cached)
CREATE TABLE IF NOT EXISTS analysis_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id UUID REFERENCES veraset_jobs(id) ON DELETE CASCADE,
  dataset_name TEXT,
  total_pings BIGINT,
  unique_devices INTEGER,
  unique_pois INTEGER,
  date_range JSONB,
  dwell_stats JSONB,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_veraset_jobs_status ON veraset_jobs(status);
CREATE INDEX IF NOT EXISTS idx_veraset_jobs_created_at ON veraset_jobs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_poi_collections_name ON poi_collections(name);
CREATE INDEX IF NOT EXISTS idx_analysis_results_dataset ON analysis_results(dataset_name);

-- Update timestamp trigger
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_poi_collections_updated_at BEFORE UPDATE ON poi_collections
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_veraset_jobs_updated_at BEFORE UPDATE ON veraset_jobs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
