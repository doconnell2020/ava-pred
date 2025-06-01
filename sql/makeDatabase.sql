DROP TABLE IF EXISTS public.av_sites;
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

DROP TABLE IF EXISTS public.station_inv;
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
