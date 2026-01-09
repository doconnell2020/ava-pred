-- Benchmark script to compare view performance
-- Run this after BuildDB.sql to test query performance
--
-- Usage: psql -d avalanche -f benchmark.sql

\timing on
\echo '=========================================='
\echo 'Performance Benchmark: Nearest Station Views'
\echo '=========================================='
\echo ''

-- Warm up the cache
\echo 'Warming up cache...'
SELECT COUNT(*) FROM av_sites;
SELECT COUNT(*) FROM station_inv;

\echo ''
\echo '--- Test 1: LATERAL JOIN view (nearest_weather_station) ---'
EXPLAIN ANALYZE
SELECT * FROM nearest_weather_station LIMIT 100;

\echo ''
\echo '--- Test 2: DISTINCT ON view (nearest_weather_station_v2) ---'
EXPLAIN ANALYZE
SELECT * FROM nearest_weather_station_v2 LIMIT 100;

\echo ''
\echo '--- Test 3: Full table scan - LATERAL JOIN ---'
EXPLAIN ANALYZE
SELECT COUNT(*) FROM nearest_weather_station;

\echo ''
\echo '--- Test 4: Full table scan - DISTINCT ON ---'
EXPLAIN ANALYZE
SELECT COUNT(*) FROM nearest_weather_station_v2;

\echo ''
\echo '=========================================='
\echo 'Benchmark complete'
\echo '=========================================='
