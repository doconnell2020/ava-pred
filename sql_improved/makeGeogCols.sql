-- Add PostGIS geography columns and spatial indexes
-- Requires PostGIS extension to be enabled

-- Add geography column to av_sites
ALTER TABLE av_sites ADD COLUMN IF NOT EXISTS geog_point geography(POINT,4326);

UPDATE av_sites
SET geog_point = ST_SetSRID(
    ST_MakePoint(longitude, latitude), 4326
)::geography
WHERE geog_point IS NULL;

DROP INDEX IF EXISTS idx_av_sites_geog_point;
CREATE INDEX idx_av_sites_geog_point ON av_sites USING GIST (geog_point);

-- Add geography column to station_inv
ALTER TABLE station_inv ADD COLUMN IF NOT EXISTS geog_point geography(POINT,4326);

UPDATE station_inv
SET geog_point = ST_SetSRID(
    ST_MakePoint(longitude_dd, latitude_dd), 4326
)::geography
WHERE geog_point IS NULL;

DROP INDEX IF EXISTS idx_station_inv_geog_point;
CREATE INDEX idx_station_inv_geog_point ON station_inv USING GIST (geog_point);

-- Analyze tables after adding indexes for query planner
ANALYZE av_sites;
ANALYZE station_inv;
