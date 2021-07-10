import configparser
import psycopg2
from fix_s3_path import to_s3
config = configparser.ConfigParser()
config.read("redshift.cfg")

rd = config["REDSHIFT"]
user, password, endpoint, port, db = rd["USER"], rd["PASSWORD"], rd["ENDPOINT"], rd["PORT"],rd["DB"]
print(user, password, endpoint, port, db)

conn = psycopg2.connect(dbname=db, user=user, password=password, host = endpoint, port = port)
conn.autocommit = True
cur = conn.cursor()

    
s3_location = to_s3(config["PATH"]["S3_OUTPUT_PATH"])

def load_to_redshift(table_name, cur, s3_location, role_arn):
    cmd = f"""
        COPY {table_name}
        FROM '{s3_location}/{table_name}'
        credentials 'aws_iam_role={role_arn}'
        region 'us-west-2'
        format csv
        delimiter ','
        IGNOREHEADER 1
        """ 
    #print(cmd)
    cur.execute(cmd)

## county locations

cur.execute("""
    DROP TABLE IF EXISTS nyt_locations_geography;
    CREATE TABLE nyt_locations_geography (
        fips    varchar(5),
        state   text,
        county  text,
        latitude    float,
        longitude   float,
        location_id    bigint NOT NULL)
    """)

load_to_redshift("nyt_locations_geography", cur, s3_location, rd["ROLE_ARN"])

# mapping from county to weather stations

cur.execute("""
    DROP TABLE IF EXISTS map_locations_stations;
    CREATE TABLE map_locations_stations (
        location_id bigint NOT NULL,
        measured varchar(4) NOT NULL,
        station_id  varchar(11) NOT NULL,
        distance float
        )
        """)

load_to_redshift("map_locations_stations", cur, s3_location, rd["ROLE_ARN"])

# weather stations

cur.execute("""
    DROP TABLE IF EXISTS weather_stations;
    CREATE TABLE weather_stations (
        station_id varchar(11) NOT NULL,
        latitude float,
        longitude float,
        elevation float,
        state text,
        station_name text  
        )
        """)
load_to_redshift("weather_stations", cur, s3_location, rd["ROLE_ARN"])


#weather records
cur.execute("""
    DROP TABLE IF EXISTS weather_records;
    CREATE TABLE weather_records (
    measured    varchar(4),
    station_id  varchar(11) NOT NULL,
    date        date,
    value       int
    )
    """)

load_to_redshift("weather_records", cur, s3_location, rd["ROLE_ARN"])

# covid per county

cur.execute("""
    DROP TABLE IF EXISTS covid_per_county;
    CREATE TABLE covid_per_county (
        date date NOT NULL,
        location_id bigint NOT NULL,
        daily_cases int,
        daily_deaths int
        )
""")
load_to_redshift("covid_per_county", cur, s3_location, rd["ROLE_ARN"])

# last covid cumulated data
cur.execute("""
    DROP TABLE IF EXISTS last_data_per_county;
    CREATE TABLE last_data_per_county (
        date date,
        location_id bigint NOT NULL,
        deaths int,
        cases  int  )
    """)

load_to_redshift("last_data_per_county", cur, s3_location, rd["ROLE_ARN"])


# count
cur.execute("select count(*) from weather_records;")
a = cur.fetchone()
print(a)

