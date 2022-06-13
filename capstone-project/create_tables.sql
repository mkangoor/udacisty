DROP TABLE IF EXISTS public.staging_immigration;
CREATE TABLE IF NOT EXISTS public.staging_immigration(
    cicid bigint,
    i94yr bigint,
    i94mon bigint,
    i94cit bigint,
    i94res bigint,
    i94port varchar(256),
    arrdate varchar(256),
    i94mode int4,
    i94addr varchar(256),
    depdate varchar(256),
    i94bir bigint,
    i94visa varchar(256),
    count numeric(18,0),
    dtadfile bigint,
    visapost varchar(256),
    occup varchar(256),
    entdepa varchar(256),
    entdepd varchar(256),
    entdepu varchar(256),
    matflag varchar(256),
    biryear int4,
    dtaddto bigint,
    gender varchar(256),
    insnum varchar(256),
    airline varchar(256),
    admnum bigint,
    fltno varchar(256),
    visatype varchar(256)
);

COPY public.staging_immigration 
FROM 's3://capstone-project-mt/immigration-data'
ACCESS_KEY_ID 'XYZ'
SECRET_ACCESS_KEY 'XYZ'
FORMAT AS Parquet 
