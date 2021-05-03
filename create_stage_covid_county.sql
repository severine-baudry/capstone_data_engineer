-- sudo -u postgres psql capstone

-- create covid_per_county_directly
DROP TABLE  IF EXISTS temp_county;

CREATE TABLE temp_county (
date date,
county text,
state  text,
fips   text,
cases  integer,
deaths integer

);
-- stage cumulated covid data into temp table
COPY temp_county FROM '/home/user/CODE/BIG_DATA/CAPSTONE_PROJECT/covid-analysis/DATA/COVID/us-counties.csv' WITH DELIMITER ',' CSV HEADER;


-- add location_id column

DROP TABLE  IF EXISTS covid_cumulated;
CREATE TABLE covid_cumulated AS 
SELECT n.*, location_id FROM  temp_county AS n LEFT JOIN nyt_locations_geography as l
ON ( (l.county = n.county) AND (l.state = n.state) AND ( (l.fips = n.fips) OR ( l.fips Is NULL AND n.fips IS NULL) ) ) 
;

-- 1170376 rows

-- create table most_recent_per_location with info from most recent date for each location_id
DROP TABLE  IF EXISTS most_recent_per_location;

CREATE TABLE most_recent_per_location AS
( WITH zuzu AS (select *, max(date) OVER(PARTITION BY location_id) AS max_date FROM covid_cumulated )
SELECT date, location_id, fips, county, state, deaths, cases FROM zuzu WHERE date = max_date);
-- 3274 rows

--create daily table

CREATE TABLE covid_per_county (
date date,
county text,
state  text,
fips   text,
cases  integer,
deaths integer
location_id bigint,
daily_cases int,
daily_deaths int
);

-- ADD COLUMN daily_deaths int ;
CREATE TABLE covid_per_county AS
WITH lagged AS 
(SELECT location_id, date, deaths, lag(deaths) OVER w AS deaths_prev, cases, lag(cases) OVER w AS cases_prev 
    FROM covid_cumulated 
    WINDOW w AS (PARTITION BY location_id ORDER BY date ASC)  
)
SELECT date, location_id, deaths - deaths_prev AS daily_deaths, cases - cases_prev AS daily_cases
FROM lagged;
