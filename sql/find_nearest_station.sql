-- Using the latitude and longitude of each event and weather station,
-- this query calculates the distance between each one and returns the 
-- closest weather station to each event. Note: some stations changed
-- their name over time and so have multiple entries, equidistant.


-- Write output to csv directly
\o '/home/ava-polars/data/nearest_stations.csv'
\timing
-- The meta-commands \a , \f , and \pset footer 
-- for unaligned, comma-separated data with no footer.
\a \f , \pset footer

SELECT * FROM nearest_weather_station;