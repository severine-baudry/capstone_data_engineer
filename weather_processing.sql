CREATE TABLE case_surveillance(
    cdc_report_dt   date,
    pos_spec_dt     date,
    onset_dt        date,
    current_status  text,
    sex         text,
    age_group   text,
    race     text,
    hosp     text,
    icu      text,
    death    text,
    medcond  text
);

 COPY case_surveillance FROM 'kwgxq_capstone/COVID-19_Case_Surveillance_Public_Use_Data.csv' WITH CSV HEADER ;


CREATE TABLE case_deaths_counties(
    date    date,
    county  text,
    state   text,
    fips    int,
    cases   int,
    deaths  int
);

COPY case_deaths_counties FROM 'kwgxq_capstone/DATA/us-counties.txt'
WITH CSV HEADER;

CREATE TABLE stage_stations(
    station varchar(100)
);

-- stage the station dat into the staging table : single column contains whole row
-- use dummy delimiter as |  (does not appear in file)
COPY stage_stations
FROM 'kwgxq_capstone/DATA/WEATHER/US_ghcnd_stations.txt'
WITH DELIMITER '|' ;

CREATE TABLE stations(
STATION_ID    varchar(11) NOT NULL,
LATITUDE      float     NOT NULL,
LONGITUDE     float NOT NULL,
ELEVATION     float ,
STATE         varchar(2) NOT NULL,
STATION_NAME          text
);

INSERT INTO stations(STATION_ID, LATITUDE, LONGITUDE, ELEVATION, STATE, STATION_NAME)
SELECT  substring(station FROM 1 FOR 11),
        cast( substring(station FROM 14 FOR 7) as float),
        cast( substring(station FROM 22 FOR 9) as float),
        cast( substring(station FROM 32 FOR 7) as float),
        substring(station FROM 39 FOR 2),
        substring(station FROM 42)
FROM  stage_stations;

-- elevation is unkwnon when set to -999.9
UPDATE stations
SET elevation = NULL
WHERE elevation = -999.9;


    SELECT cn.fips, cn.state, cn.county, 
        sum(cn.cases) AS cases, sum(cn.deaths) AS deaths, 
        sum(cn.cases)::float/cov.cases AS ratio_cases, sum(cn.deaths)::float/cov.deaths AS ratio_deaths
    FROM case_deaths_counties AS cn, covid_total AS cov
    GROUP BY cn.fips, cn.state, cn.county, cov.cases, cov.deaths
    ORDER BY ratio_cases DESC, ratio_deaths DESC;

-- function to display a float in scientific notation :

CREATE FUNCTION dispsci(numeric) RETURNS text AS $$
    SELECT to_char($1, '9D99EEEE')
$$ LANGUAGE SQL;

--create view grouped by county
CREATE VIEW group_county AS 
SELECT county, state, fips, sum(cases) as cases, sum(deaths) as deaths
FROM case_deaths_counties  WHERE deaths IS NOT NULL
GROUP BY fips, county, state;

-- counties with most cases and deaths, ratio with the total US number
SELECT county, state, fips, dispsci(g.cases::numeric/c.cases) AS cases, dispsci(g.deaths::numeric/c.deaths) AS deaths
FROM group_county as g,covid_total AS c
ORDER BY g.cases DESC, g.deaths DESC;

-- add rank by number of deaths
SELECT county, state, fips, dispsci(g.cases::numeric/c.cases) AS cases, dispsci(g.deaths::numeric/c.deaths) AS deaths, rank() OVER  (ORDER BY g.deaths DESC)
FROM group_county as g,covid_total AS c
ORDER BY g.cases DESC, g.deaths DESC;

-- get ranks of data with no fips
SELECT * FROM ratio_rank WHERE fips IS NULL;

CREATE VIEW geo_anomalies AS (SELECT *, (CASE  WHEN fips IS NULL THEN 0  ELSE 1 END) AS fips_exist
FROM case_deaths_counties ) ;

CREATE VIEW geo_anomalies AS (SELECT *, (CASE  WHEN fips IS NULL THEN 0  ELSE 1 END) AS fips_exist
FROM group_county ) ;

-- pivot : deaths per state, for locations with and without fips
CREATE TABLE deaths_per_state AS SELECT * FROM CROSSTAB(
    'SELECT state, fips_exist, deaths FROM group_geo ORDER BY 1,2;',
    'SELECT DISTINCT(fips_exist) FROM group_geo ORDER BY 1'
    )
    AS deaths_per_state(state text, no_fips int, fips_exist int);

CREATE TABLE cases_per_state AS SELECT * FROM CROSSTAB(
    'SELECT state, fips_exist, cases FROM group_geo ORDER BY 1,2;',
    'SELECT DISTINCT(fips_exist) FROM group_geo ORDER BY 1'
    )
    AS cases_per_state(state text, no_fips int, fips_exist int);

CREATE TABLE gazeeter(
USPS	varchar(2),
GEOID	int,
ANSICODE	varchar(8),
NAME	ALAND	AWATER	ALAND_SQMI	AWATER_SQMI	INTPTLAT	INTPTLONG 
);

-- load stations for whole world to search for puerto rico 
DELETE  FROM stage_stations ;
COPY stage_stations
FROM 'kwgxq_capstone/DATA/WEATHER/ghcnd-stations.txt' WITH DELIMITER '|' ;
-- create table for whole world with same structure as US stations table
CREATE table world_stations ( LIKE stations  INCLUDING ALL);

INSERT INTO world_stations(STATION_ID, LATITUDE, LONGITUDE, ELEVATION, STATE, STATION_NAME)
SELECT  substring(station FROM 1 FOR 11),
        cast( substring(station FROM 14 FOR 7) as float),
        cast( substring(station FROM 22 FOR 9) as float),
        cast( substring(station FROM 32 FOR 7) as float),
        substring(station FROM 39 FOR 2),
        substring(station FROM 42)
FROM  stage_stations;

GRANT ALL ON TABLE stations TO sb;
GRANT ALL PRIVILEGES ON world_stations TO sb;
