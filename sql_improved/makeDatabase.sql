-- Create tables for avalanche prediction database
-- This file defines the core table structures with proper constraints

DROP TABLE IF EXISTS public.av_sites CASCADE;
CREATE TABLE public.av_sites (
    id SERIAL PRIMARY KEY,
    ob_date date NOT NULL,
    location_coords character varying(100),
    location_coords_type character varying(100),
    latitude numeric(20,14) NOT NULL,
    longitude numeric(20,14) NOT NULL
);

CREATE INDEX idx_av_sites_ob_date ON av_sites (ob_date);

-- Weather station inventory table
DROP TABLE IF EXISTS public.station_inv CASCADE;
CREATE TABLE public.station_inv (
    station_id character varying(10) PRIMARY KEY,
    station_name character varying(100) NOT NULL,
    province character varying(50),
    climate_id character varying(20),
    wmo_id character varying(10),
    tc_id character varying(3),
    latitude_dd numeric(5,2) NOT NULL,
    longitude_dd numeric(5,2) NOT NULL,
    lat numeric(10,0),
    long numeric(10,0),
    elevation_m numeric(6,2),
    first_year integer,
    last_year integer,
    hly_first_year integer,
    hly_last_year integer,
    dly_first_year integer,
    dly_last_year integer,
    mly_first_year integer,
    mly_last_year integer
);

CREATE INDEX idx_station_inv_years ON station_inv (first_year, last_year);
