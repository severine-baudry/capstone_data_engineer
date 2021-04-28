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
    
    create_temp_county_table = PostgresOperator(
        task_id="create_temp_county_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS temp_county (
                date date,
                county text,
                state text,
                fips text,
                cases int,
                deaths int 
                );
                """,
        )
    
    load_temp_county_table = PostgresOperator(
        task_id = "load_temp_county_table",
        postgres_conn_id = "postgres_default",
        sql = """
            COPY temp_county FROM '/tmp/us-counties-recent.csv' WITH CSV HEADER ;
        """
        )
    filter_date = PostgresOperator(
        task_id = "filter_date",
        postgres_conn_id = "postgres_default",
        sql = """
            DELETE FROM temp_county WHERE date > ( DATE '2021-04-10') ;
        """
        )
    
    filter_locations = PostgresOperator(
        task_id = "filter_location",
        postgres_conn_id = "postgres_default",
        sql = """
             CREATE TABLE temp_county_2 AS
            (   SELECT date, location_id, deaths, cases
                FROM temp_county AS n JOIN nyt_locations_geography as l
                ON  (l.county = n.county) AND (l.state = n.state) AND ( (l.fips = n.fips) OR ( l.fips Is NULL AND n.fips IS NULL) )
            
            );

        """
        )
    compute_daily_stats = PostgresOperator(
        task_id = "compute_daily_stats",
        postgres_conn_id = "postgres_default",
        sql = """
            CREATE TABLE temp_daily AS
            WITH zuzu AS (SELECT location_id, date, deaths, lag(deaths) OVER w AS deaths_prev, cases, lag(cases) OVER w AS cases_prev 
                FROM temp_county_2 
                WINDOW w AS (PARTITION BY location_id ORDER BY date ASC)  )
            SELECT location_id, deaths, deaths_prev, deaths-deaths_prev AS daily_deaths, cases, cases_prev, cases - cases_prev AS daily_cases FROM zuzu;
            """
            )
    
    drop_temp_county_table = PostgresOperator(
        task_id = "drop_temp_county_table",
        postgres_conn_id = "postgres_default",
        sql = """
        DROP TABLE IF EXISTS temp_county;
        """
        )
    drop_temp_county_2_table = PostgresOperator(
        task_id = "drop_temp_county_2_table",
        postgres_conn_id = "postgres_default",
        sql = """
        DROP TABLE IF EXISTS temp_county_2;
        """
        )
    
    
[ drop_temp_county_table, drop_temp_county_2_table] >> create_temp_county_table >> load_temp_county_table >> filter_date >> filter_locations 
