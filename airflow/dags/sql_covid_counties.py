
q_create_recent_per_county_table = """
CREATE TABLE IF NOT EXISTS recent_per_county (
    date date,
    county text,
    state text,
    fips text,
    cases int,
    deaths int 
    );
"""

q_load_recent_per_county_table = """
    COPY recent_per_county 
    FROM '/tmp/us-counties-recent.csv' 
    WITH CSV HEADER ;
"""

q_add_location_id = """
    ALTER TABLE recent_per_county ADD COLUMN location_id bigint;
    UPDATE recent_per_county AS new
    SET location_id = loc.location_id
    FROM nyt_locations_geography AS loc
    WHERE ( (loc.county = new.county) AND (loc.state = new.state) AND ( (loc.fips = new.fips) OR ( loc.fips Is NULL AND new.fips IS NULL) ) );
"""

q_retrieve_past_data = """
    WITH min_recent AS 
    ( WITH t0 AS 
        (SELECT *, rank() OVER (PARTITION BY location_id ORDER BY date) FROM recent_per_county)
        SELECT * FROM t0  WHERE rank = 1
    )
    INSERT INTO recent_per_county(location_id, state, county, fips, date, cases, deaths)
    SELECT  most_recent_per_location.location_id, most_recent_per_location.state, most_recent_per_location.county, most_recent_per_location.fips, most_recent_per_location.date, most_recent_per_location.cases, most_recent_per_location.deaths 
    FROM most_recent_per_location JOIN min_recent 
    ON most_recent_per_location.location_id = min_recent.location_id
    WHERE most_recent_per_location.date < min_recent.date;        
"""

q_compute_daily_stats = """
    ALTER TABLE recent_per_county 
    ADD COLUMN daily_cases int,
    ADD COLUMN daily_deaths int ;
    WITH prev AS 
    (SELECT location_id, date, lag(deaths) OVER w AS deaths_prev, lag(cases) OVER w AS cases_prev 
        FROM recent_per_county 
        WINDOW w AS (PARTITION BY location_id ORDER BY date ASC)  
    )
    UPDATE recent_per_county AS new
    SET daily_cases = new.cases - prev.cases_prev , daily_deaths = new.deaths - prev.deaths_prev
    FROM prev AS prev
    WHERE new.date = prev.date AND new.location_id = prev.location_id;
"""

q_filter_date = """
    ALTER TABLE recent_per_county 
    ADD COLUMN last date;
    UPDATE recent_per_county 
    SET last = most_recent_per_location.date
    FROM most_recent_per_location 
    WHERE most_recent_per_location.location_id = recent_per_county.location_id;
    DELETE FROM recent_per_county
    WHERE date <= last;
    ALTER TABLE recent_per_county
    DROP COLUMN last;    
"""

q_update_last_date = """
    CREATE TABLE new_recent AS

    WITH tmp0 AS
    (
    SELECT * FROM most_recent_per_location UNION SELECT date, location_id, fips, county, state, deaths, cases FROM recent_per_county
    ),
    tmp1 AS            
    (
        SELECT *, rank() OVER (PARTITION BY location_id ORDER BY date DESC)
        FROM tmp0
    )
    SELECT date, location_id, fips, county, state, deaths, cases
    FROM tmp1
    WHERE rank =1;
    
    DROP TABLE most_recent_per_location;
    ALTER TABLE new_recent RENAME TO most_recent_per_location;
"""

q_append_full_per_county = """
    INSERT INTO covid_per_county(date, location_id, daily_cases, daily_deaths)
    SELECT date, location_id, daily_cases, daily_deaths
    FROM recent_per_county;
"""
q_drop_recent_per_county_table = """
    DROP TABLE IF EXISTS recent_per_county;
"""

