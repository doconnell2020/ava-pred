DROP TABLE IF EXISTS  public.av_sites CASCADE;
CREATE TABLE public.av_sites (
    ob_date date,
    location_coords character varying(100),
    location_coords_type character varying(100),
    latitude numeric(20,14),
    longitude numeric(20,14)
);


ALTER TABLE public.av_sites OWNER TO david;

-- TOC entry 208 (class 1259 OID 31442)
-- Name: station_inv; Type: TABLE; Schema: public; Owner: david

DROP TABLE IF EXISTS public.station_inv CASCADE;
CREATE TABLE public.station_inv (
    station_name character varying(100) NOT NULL,
    province character varying(50),
    climate_id character varying(20),
    station_id character varying(10),
    wmo_id character varying(10),
    tc_id character varying(3),
    latitude_dd numeric(5,2),
    longitude_dd numeric(5,2),
    lat numeric(10,0),
    long numeric(10,0),
    eleavation_m numeric(6,2),
    first_year integer,
    last_year integer,
    hly_first_year integer,
    hly_last_year integer,
    dly_first_year integer,
    dly_last_year integer,
    mly_first_year integer,
    mly_last_year integer
);


ALTER TABLE public.station_inv OWNER TO david;


--Add avalanche info 
COPY av_sites FROM 
'/home/david/Documents/ARU/AvalancheProject/demo/data/can_avs_lat_long_date.csv' 
WITH (FORMAT csv, HEADER true);

--Add weather station info
COPY station_inv FROM 
'/home/david/Documents/ARU/AvalancheProject/demo/data/station_inv.csv' 
WITH (FORMAT csv, HEADER true);

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


-- Set a View which calculates the nearest weather station to the avalanche event
CREATE OR REPLACE VIEW nearest_weather_station AS
	SELECT 
	  av_sites.ob_date, 
	  station_inv.station_name,
	  CAST ((ST_Distance(av_sites.geog_point, station_inv.geog_point)/1000) as numeric (7,3)) as "distance(km)",
	  station_id
	FROM 
	  av_sites 
	  CROSS JOIN station_inv
	  JOIN (
		SELECT 
		  ob_date, 
		  MIN(ST_Distance(av_sites.geog_point, station_inv.geog_point)) as min_distance
		FROM 
		  av_sites 
		  CROSS JOIN station_inv 
		WHERE 
		  ST_Distance(av_sites.geog_point, station_inv.geog_point) IS NOT NULL
		  AND EXTRACT(YEAR FROM av_sites.ob_date) BETWEEN station_inv.first_year AND station_inv.last_year
		GROUP BY 
		  ob_date
	  ) as sub_t 
	  ON av_sites.ob_date = sub_t.ob_date 
	  AND ST_Distance(av_sites.geog_point, station_inv.geog_point) = sub_t.min_distance
	  ORDER BY "distance(km)" DESC;
