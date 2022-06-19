DROP TABLE IF EXISTS public.staging_immigration;
CREATE TABLE IF NOT EXISTS public.staging_immigration(
    cicid DOUBLE PRECISION,
    i94yr DOUBLE PRECISION, 
    i94mon DOUBLE PRECISION, 
    i94cit DOUBLE PRECISION, 
    i94res DOUBLE PRECISION, 
    i94port varchar(256), 
    arrdate DOUBLE PRECISION,
    i94mode DOUBLE PRECISION,
    i94addr varchar(256),
    depdate DOUBLE PRECISION,
    i94bir DOUBLE PRECISION, 
    i94visa DOUBLE PRECISION,
    "count" DOUBLE PRECISION,
    dtadfile varchar(256),
    visapost varchar(256), 
    occup varchar(256),
    entdepa varchar(256),
    entdepd varchar(256),
    entdepu varchar(256),
    matflag varchar(256),
    biryear DOUBLE PRECISION, 
    dtaddto varchar(256), 
    gender varchar(256), 
    insnum varchar(256), 
    airline varchar(256), 
    admnum DOUBLE PRECISION,
    fltno varchar(256), 
    visatype varchar(256)
);

DROP TABLE IF EXISTS public.immigration;
CREATE TABLE IF NOT EXISTS public.immigration(
    admission_no BIGINT,
    cic_id BIGINT,
  	passenger_id VARCHAR(256),
  	flight_id VARCHAR(256),
  	visa_id VARCHAR(256),
  	year BIGINT,
	month BIGINT,
  	day BIGINT,
  	travel_model BIGINT,
  	"count" BIGINT,
  	PRIMARY KEY(admission_no)
);

DROP TABLE IF EXISTS public.passenger;
CREATE TABLE IF NOT EXISTS public.passenger(
  	passenger_id VARCHAR(256),
    insnum VARCHAR(256),
    gender VARCHAR(256),
  	years BIGINT,
  	birth_year BIGINT,
    occupation VARCHAR(256),
  	state_of_residence_abb VARCHAR(256),
  	state_of_residence VARCHAR(256),
  	PRIMARY KEY(passenger_id)
);

DROP TABLE IF EXISTS public.flights;
CREATE TABLE IF NOT EXISTS public.flights(
  	flight_id VARCHAR(256),
  	flight_no VARCHAR(256),
  	dep_country_id BIGINT,
  	dep_country VARCHAR(256), 
  	arr_country_id BIGINT,
  	arr_country VARCHAR(256),
  	airport_city_abb VARCHAR(256),
  	airport_city_name VARCHAR(256),
  	state_of_residence_abb VARCHAR(256),
  	dep_date VARCHAR(256),
  	arr_date VARCHAR(256),
  	PRIMARY KEY(flight_id)
);

DROP TABLE IF EXISTS public.flight_flags;
CREATE TABLE IF NOT EXISTS public.flight_flags(
  	cic_id BIGINT,
  	admission_no BIGINT,
  	arr_flag VARCHAR(256),
  	dep_flag VARCHAR(256),
  	upd_flag VARCHAR(256),
  	match_flag VARCHAR(256),
  	PRIMARY KEY(cic_id)
);

DROP TABLE IF EXISTS public.visas;
CREATE TABLE IF NOT EXISTS public.visas(
  	visa_id VARCHAR(256),
  	visa_type_id BIGINT,
  	visa_type_class VARCHAR(256),
  	visa_type VARCHAR(256),
  	visa_post VARCHAR(256),
  	admitted_date VARCHAR(256),
  	PRIMARY KEY(visa_id)
);

DROP TABLE IF EXISTS public.airport_code;
CREATE TABLE IF NOT EXISTS public.airport_code(
 	ident VARCHAR(256),
 	type VARCHAR(256),
 	name VARCHAR(256),
 	elevation_ft BIGINT,
 	continent VARCHAR(256),
 	iso_country VARCHAR(256),
 	iso_region VARCHAR(256),
 	municipality VARCHAR(256),
 	gps_code VARCHAR(256),
 	iata_code VARCHAR(256),
 	local_code VARCHAR(256),
 	coordinates VARCHAR(256)
);

DROP TABLE IF EXISTS public.airport_city;
CREATE TABLE IF NOT EXISTS public.airport_city(
  	key VARCHAR(256),
  	airport_city VARCHAR(256)
);

DROP TABLE IF EXISTS public.country_codes;
CREATE TABLE IF NOT EXISTS public.country_codes(
  	key BIGINT,
  	country VARCHAR(256)
);

DROP TABLE IF EXISTS public.airport_type;
CREATE TABLE IF NOT EXISTS public.airport_type(
  	key BIGINT,
  	i94model VARCHAR(256)
);

DROP TABLE IF EXISTS public.states;
CREATE TABLE IF NOT EXISTS public.states(
  	key VARCHAR(256),
  	state_of_residence VARCHAR(256)
);

DROP TABLE IF EXISTS public.visa_type;
CREATE TABLE IF NOT EXISTS public.visa_type(
  	key BIGINT,
  	visa_type_class VARCHAR(256)
);