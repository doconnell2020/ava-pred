-- Export nearest weather station data to CSV
-- Uses the optimized view for finding closest station to each avalanche event
--
-- Usage: psql -d avalanche -f find_nearest_station.sql
-- Or set output_file variable:
--   \set output_file '/path/to/output.csv'

\if :{?output_file}
\else
    \set output_file '/home/ava-polars/data/nearest_stations.csv'
\endif

\o :output_file
\timing

-- Configure for CSV output
\a
\f ,
\pset footer off

-- Use the optimized LATERAL JOIN view
SELECT ob_date, station_name, "distance(km)", station_id
FROM nearest_weather_station;

\o
\pset footer on
