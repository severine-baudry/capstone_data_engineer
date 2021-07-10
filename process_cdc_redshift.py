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

# stage covid per population group
print("stage fact table : covid cases per population group")
cur.execute("""
    DROP TABLE IF EXISTS stage_cdc;
    CREATE TABLE stage_cdc (
        cdc_case_earliest_dt    text,
        sex   text,
        age_group  text,
        race_ethnicity_combined    text,
        death_yn   text,
        count    bigint)
    """)

cmd = f"""
    COPY stage_cdc
    FROM '{s3_location}/covid_by_pop_group.json'
    credentials 'aws_iam_role={rd["ROLE_ARN"]}'
    region 'us-west-2'
    json 'auto'
    """ 
cur.execute(cmd)

# age dimension table
print("loading age dimension table")
cur.execute("""
    DROP TABLE IF EXISTS dim_age;
    CREATE TABLE dim_age (
        age_group_id int NOT NULL,
        age_group text
        )
    """)

cur.execute("""
    INSERT INTO  dim_age VALUES
        (0,  '0 - 9 Years'),
        (10, '10 - 19 Years'),
        (20, '20 - 29 Years'),
        (30, '30 - 39 Years'),
        (40, '40 - 49 Years'),
        (50, '50 - 59 Years'),
        (60, '60 - 69 Years'),
        (70, '70 - 79 Years'),
        (80, '80+ Years'),
        (1000,'Missing'),
        (2000,'NA')
    """)

# race and ethnicity dimension table
print("loading race dimension table")
cur.execute("""
    DROP TABLE IF EXISTS dim_race_ethnicity;
    CREATE TABLE dim_race_ethnicity (
        race_ethnicity_id int NOT NULL,
        race text,
        Hispanic_Latino boolean
        )
    """)

cur.execute("""
    INSERT INTO  dim_race_ethnicity VALUES
        (0, 'American Indian/Alaska Native', False),
        (1, 'Asian', False),
        (2, 'Black', False),
        (3, 'Native Hawaiian/Other Pacific Islander', False),
        (4, 'White', False),
        (5, 'Hispanic/Latino', True),
        (10, 'Multiple/Other', NULL),
        (1000, 'Missing', NULL),
        (2000, 'NA', NULL),
        (3000, 'Unknown', NULL)
    """)

# sex dimension table
print("loading sex dimension table")
cur.execute("""
    DROP TABLE IF EXISTS dim_sex;
    CREATE TABLE dim_sex (
        sex_id int NOT NULL,
        sex text
        )
    """)

cur.execute("""
    INSERT INTO  dim_sex VALUES
        (0, 'Female'), 
        (1, 'Male'), 
        (2, 'Other'), 
        (1000, 'Missing'), 
        (2000, 'NA'), 
        (3000, 'Unknown') 
    """)

# death status dimension table
print("loading death dimension table")
cur.execute("""
    DROP TABLE IF EXISTS dim_death;
    CREATE TABLE dim_death (
        death_status_id int NOT NULL,
        death_status text
        )
    """)

cur.execute("""
    INSERT INTO  dim_death VALUES
        (0, 'No'), 
        (1, 'Yes'), 
        (1000, 'Missing'), 
        (3000, 'Unknown') 
    """)

# fact table : join staged covid table with dimension tables
print("loading fact table")
cur.execute("""
    DROP TABLE IF EXISTS covid_per_popgroup;
    CREATE TABLE covid_per_popgroup AS(

    WITH temp_covid AS(
        SELECT to_date(cdc_case_earliest_dt, 'YYYY-MM-DDTHH:MI:SS.MS') AS cdc_case_earliest_dt,
        SPLIT_PART(race_ethnicity_combined, ',', 1) AS race,
        sex, age_group, death_yn, count
        FROM stage_cdc
        )
    SELECT cdc_case_earliest_dt, sex_id, age_group_id, race_ethnicity_id, death_status_id, count 
    FROM temp_covid 
    LEFT OUTER JOIN dim_sex ON temp_covid.sex = dim_sex.sex
    LEFT OUTER JOIN dim_age ON temp_covid.age_group = dim_age.age_group
    LEFT OUTER JOIN dim_race_ethnicity ON temp_covid.race = dim_race_ethnicity.race
    LEFT OUTER JOIN dim_death ON temp_covid.death_yn = dim_death.death_status
    )
   
   
    """)

# check that all values in the fact table are in dimension tables

cur.execute("""
    SELECT count(*) FROM covid_per_popgroup
    WHERE (cdc_case_earliest_dt IS NULL )
        OR (sex_id IS NULL)
        OR (age_group_id IS NULL)
        OR (race_ethnicity_id IS NULL)
        OR (death_status_id IS NULL)
"""
)
res = cur.fetchone()
if res[0] !=0 :
    print(f"Error : there are {res} rows with null values")
    cur.execute("""
        SELECT * FROM covid_per_popgroup
        WHERE (cdc_case_earliest_dt IS NULL )
            OR (sex_id IS NULL)
            OR (age_group_id IS NULL)
            OR (race_ethnicity_id IS NULL)
            OR (death_status_id IS NULL)
    """
    )
    res = cur.fetchone()
    print(res)
    
else :
    print("check ok")
    

# drop staging table
cur.execute("""
    DROP TABLE IF EXISTS stage_cdc;
""")
