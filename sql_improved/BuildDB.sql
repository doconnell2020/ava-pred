-- Main build script for avalanche prediction database
-- Composes the database from modular SQL files
--
-- Usage: psql -d avalanche -f BuildDB.sql
-- Or set DATA_DIR variable before running:
--   \set data_dir '/path/to/data'

-- Default data directory (override with \set data_dir before running)
\if :{?data_dir}
\else
    \set data_dir '/home/david/Documents/ARU/AvalancheProject/demo/data'
\endif

-- Create tables
\i makeDatabase.sql

-- Load avalanche site data
\set av_file :data_dir '/can_avs_lat_long_date.csv'
COPY av_sites (ob_date, location_coords, location_coords_type, latitude, longitude)
FROM :'av_file'
WITH (FORMAT csv, HEADER true);

-- Load weather station inventory
\set station_file :data_dir '/station_inv.csv'
COPY station_inv (station_name, province, climate_id, station_id, wmo_id, tc_id,
                  latitude_dd, longitude_dd, lat, long, elevation_m,
                  first_year, last_year, hly_first_year, hly_last_year,
                  dly_first_year, dly_last_year, mly_first_year, mly_last_year)
FROM :'station_file'
WITH (FORMAT csv, HEADER true);

-- Add geography columns and indexes
\i makeGeogCols.sql

-- Create optimized view for finding nearest weather station
-- Uses LATERAL JOIN with KNN operator (<->) for efficient spatial lookup
DROP VIEW IF EXISTS nearest_weather_station;
CREATE VIEW nearest_weather_station AS
SELECT
    av.id AS av_site_id,
    av.ob_date,
    nearest.station_name,
    nearest.station_id,
    CAST((nearest.distance / 1000) AS numeric(7,3)) AS "distance(km)"
FROM av_sites av
CROSS JOIN LATERAL (
    SELECT
        si.station_name,
        si.station_id,
        ST_Distance(av.geog_point, si.geog_point) AS distance
    FROM station_inv si
    WHERE av.geog_point IS NOT NULL
      AND si.geog_point IS NOT NULL
      AND (
          -- Station was operational during the avalanche year
          (si.first_year IS NULL AND si.last_year IS NULL)
          OR (si.first_year IS NULL AND EXTRACT(YEAR FROM av.ob_date) <= si.last_year)
          OR (si.last_year IS NULL AND EXTRACT(YEAR FROM av.ob_date) >= si.first_year)
          OR EXTRACT(YEAR FROM av.ob_date) BETWEEN si.first_year AND si.last_year
      )
    ORDER BY av.geog_point <-> si.geog_point
    LIMIT 1
) AS nearest
ORDER BY "distance(km)" DESC;

-- Alternative view using DISTINCT ON (for comparison testing)
DROP VIEW IF EXISTS nearest_weather_station_v2;
CREATE VIEW nearest_weather_station_v2 AS
SELECT DISTINCT ON (av.id)
    av.id AS av_site_id,
    av.ob_date,
    si.station_name,
    si.station_id,
    CAST((ST_Distance(av.geog_point, si.geog_point) / 1000) AS numeric(7,3)) AS "distance(km)"
FROM av_sites av
JOIN station_inv si ON
    av.geog_point IS NOT NULL
    AND si.geog_point IS NOT NULL
    AND (
        (si.first_year IS NULL AND si.last_year IS NULL)
        OR (si.first_year IS NULL AND EXTRACT(YEAR FROM av.ob_date) <= si.last_year)
        OR (si.last_year IS NULL AND EXTRACT(YEAR FROM av.ob_date) >= si.first_year)
        OR EXTRACT(YEAR FROM av.ob_date) BETWEEN si.first_year AND si.last_year
    )
ORDER BY av.id, av.geog_point <-> si.geog_point;
