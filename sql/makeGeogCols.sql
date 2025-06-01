-- Alter the tables in db avalanche to have geography data
-- Starting with av_sites table
ALTER TABLE av_sites ADD COLUMN geog_point geography(POINT,4326);
UPDATE av_sites
SET geog_point =
ST_SetSRID(
ST_MakePoint(av_sites.longitude,av_sites.latitude),4326
)::geography;
CREATE INDEX avs_idx ON av_sites USING GIST (geog_point);

-- Alter the station_inv table too
ALTER TABLE station_inv ADD COLUMN geog_point geography(POINT,4326);
UPDATE station_inv
SET geog_point =
ST_SetSRID(
ST_MakePoint(station_inv.longitude_dd, station_inv.latitude_dd),4326
)::geography;
CREATE INDEX stations_idx ON station_inv USING GIST (geog_point);