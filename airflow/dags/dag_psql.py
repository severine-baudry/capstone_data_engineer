from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.macros import ds_add, ds_format

import os
from datetime import datetime, timedelta

default_args = {"owner": "airflow"}

with DAG(
    dag_id='dag_psql',
    default_args=default_args,
    schedule_interval='@monthly',
    start_date=  datetime(2021,4,27), #days_ago(2), #datetime.datetime.now(), #days_ago(2),
    tags = ["covid", "psql"],
    ) as dag:
    
    create_recent_per_county_table = PostgresOperator(
        task_id="create_recent_per_county_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS recent_per_county (
                date date,
                county text,
                state text,
                fips text,
                cases int,
                deaths int 
                );
                """,
        )
    
    load_recent_per_county_table = PostgresOperator(
        task_id = "load_recent_per_county_table",
        postgres_conn_id = "postgres_default",
        sql = """
            COPY recent_per_county FROM '/tmp/us-counties-recent.csv' WITH CSV HEADER ;
        """
        )
    # add location_id key to new data
    add_location_id = PostgresOperator(
        task_id = "add_location_id",
        postgres_conn_id = "postgres_default",
        sql =  """
            ALTER TABLE recent_per_county ADD COLUMN location_id bigint;
            UPDATE recent_per_county AS new
            SET location_id = loc.location_id
            FROM nyt_locations_geography AS loc
            WHERE ( (loc.county = new.county) AND (loc.state = new.state) AND ( (loc.fips = new.fips) OR ( loc.fips Is NULL AND new.fips IS NULL) ) );
            """
            )
    # to compute daily data from cumulated data, it is necessary to have the previous cumulated data
    # for some counties with very few cases / deaths, previous data does not appear in the 1-month window
    # we must thus retrieve it from past data and add it to recent data
    # this is done in 2 phases :
    #   - First, get min(date) in recent data for each location_id (table min_recent)
    #   - Then, compare with max(date) for past_data : 
    #       - if min(recent) > max(past) : add max(past) for location_id to recent table 
    retrieve_past_data = PostgresOperator(
        task_id = "retrieve_past_data",
        postgres_conn_id = "postgres_default",
        sql =  """
            WITH min_recent AS 
            ( WITH t0 AS 
                (SELECT *, rank() OVER (PARTITION BY location_id ORDER BY date) FROM recent_per_county)
                SELECT * FROM t0  WHERE rank = 1
            )
            INSERT INTO recent_per_county(location_id, state, county, fips, date, cases, deaths)
            SELECT  last_data.location_id, last_data.state, last_data.county, last_data.fips, last_data.date, last_data.cases, last_data.deaths 
            FROM last_data JOIN min_recent 
            ON last_data.location_id = min_recent.location_id
            WHERE last_data.date < min_recent.date;        
        """
        )   
    
    # compute daily deaths and cases from cumulated values :
    # - partition by location_id and order by date
    # - compute previous cases and deaths by "shifting" the case and deaths column
    # - compute daily data : cumulated_current - cumulated_prev
    compute_daily_stats = PostgresOperator(
        task_id = "compute_daily_stats",
        postgres_conn_id = "postgres_default",
        sql = """
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
            )
             
            
    filter_date = PostgresOperator(
        task_id = "filter_date",
        postgres_conn_id = "postgres_default",
        sql = """
            ALTER TABLE recent_per_county 
            ADD COLUMN last date;
            UPDATE recent_per_county 
            SET last = last_data.date
            FROM last_data 
            WHERE last_data.location_id = recent_per_county.location_id;
            DELETE FROM recent_per_county
            WHERE date <= last;
            ALTER TABLE recent_per_county
            DROP COLUMN last;
            
        """
        )
    update_last_date = PostgresOperator(
        task_id = "update_last_date",
        postgres_conn_id = "postgres_default",
        sql = """
            DROP TABLE IF EXISTS last_data;
            
            CREATE TABLE last_data AS
                (SELECT date, county, state, fips, cases, deaths , rank() over (PARTITION BY location_id ORDER BY date DESC)
                FROM recent_per_county
                );
            DELETE FROM last_data
            WHERE rank > 1;
            ALTER TABLE last_data DROP COLUMN rank;            
        """
        )
    append_full_per_county = PostgresOperator(
        task_id = "append_full_per_county",
        postgres_conn_id = "postgres_default",
        sql = """
        INSERT INTO covid_per_county(date, location_id, daily_cases, daily_deaths)
        SELECT date, location_id, daily_cases, daily_deaths
        FROM recent_per_county;
        """
        )
    
    drop_recent_per_county_table = PostgresOperator(
        task_id = "drop_recent_per_county_table",
        postgres_conn_id = "postgres_default",
        sql = """
        DROP TABLE IF EXISTS recent_per_county;
        """
        )

    
    
drop_recent_per_county_table >> create_recent_per_county_table >> load_recent_per_county_table >> add_location_id >> retrieve_past_data >> compute_daily_stats >> filter_date >> append_full_per_county >> update_last_date
