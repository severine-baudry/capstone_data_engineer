import sys
print(sys.path)
from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.macros import ds_add, ds_format
from operators.data_download import dummy_download, download_diff_weather

import os
from datetime import datetime, timedelta


args = {
    'owner': 'Airflow',
}

sql_filter = """
            DROP TABLE IF EXISTS {{params.filtered_table}};
            CREATE TABLE {{params.filtered_table}} AS
            WITH temp AS (
                SELECT * FROM {{params.full_table}}
                WHERE quality_flag IS NULL
                )
            SELECT temp.measured, temp.station_id,  temp.date, temp.value 
            FROM temp JOIN {{params.selected_stations}}  AS sel
            ON sel.station_id = temp.station_id AND sel.measured = temp.measured

            """

sql_create_stage_table = """
            DROP TABLE IF EXISTS {{params.stage_table}};
            CREATE TABLE IF NOT EXISTS {{params.stage_table}}(
                station_id varchar(12),
                date varchar(8),
                measured varchar(4),
                value int, 
                measurement_flag varchar(1), 
                quality_flag varchar(1), 
                source_flag varchar(1), 
                hour varchar(6)
            );
            
            COPY {{params.stage_table}} 
            FROM \'{{ti.xcom_pull(key='weather_diff_dir')}}/{{params.stage_file}}\' 
            WITH CSV HEADER
            """  


with DAG(
    dag_id='daily_weather_2',
    default_args=args,
    schedule_interval= '@once', #'@monthly', # for testing #  @daily',
    start_date= datetime(2021,5,2), #days_ago(2), #datetime.datetime.now(), #days_ago(2),
#    end_date= datetime(2021,5,2), #days_ago(2), #datetime.datetime.now(), #days_ago(2),
    max_active_runs = 1,
    tags=['covid'],
) as dag:
    
        #download_diff_weather_task = download_diff_weather(task_id = "download_diff_weather")
        download_diff_weather_task = dummy_download(task_id = "download_diff_weather")
        # jinja + xcom + task parameters + sql string is uber relou
        stage_recent_insert = PostgresOperator(
            task_id = "stage_recent_insert",
            postgres_conn_id = "postgres_default",
            sql= sql_create_stage_table,
            params = { "stage_table" : "recent_insert",
                      "stage_file" : "insert.csv"
                    }
            )
        stage_recent_delete = PostgresOperator(
            task_id = "stage_recent_delete",
            postgres_conn_id = "postgres_default",
            sql=sql_create_stage_table,
            params = { "stage_table" : "recent_delete",
                      "stage_file" : "delete.csv"
                    }
            )
        stage_recent_update = PostgresOperator(
            task_id = "stage_recent_update",
            postgres_conn_id = "postgres_default",
            sql=sql_create_stage_table,
            params = { "stage_table" : "recent_update",
                      "stage_file" : "update.csv"
                    }          
            )
        
        filter_insert = PostgresOperator(
            task_id = "filter_insert",
            postgres_conn_id = "postgres_default",
            sql = sql_filter,
            params= {"full_table" : "recent_insert", 
                     "filtered_table" : "recent_insert_filtered",
                     "selected_stations" : "weatherelem_station"}
            )
        
        filter_delete = PostgresOperator(
            task_id = "filter_delete",
            postgres_conn_id = "postgres_default",
            sql = sql_filter,
            params= {"full_table" : "recent_delete", 
                     "filtered_table" : "recent_delete_filtered",
                     "selected_stations" : "weatherelem_station"}
            )
        filter_update = PostgresOperator(
            task_id = "filter_update",
            postgres_conn_id = "postgres_default",
            sql = sql_filter,
            params= {"full_table" : "recent_update", 
                     "filtered_table" : "recent_update_filtered",
                     "selected_stations" : "weatherelem_station"}
            )
        
        process_delete = PostgresOperator(
            task_id = "process_delete",
            postgres_conn_id = "postgres_default",
            sql = """
                DELETE FROM weather_data
                USING recent_delete_filtered AS r
                WHERE (weather_data.measured IS NOT DISTINCT FROM r.measured) AND
                    (weather_data.station_id IS NOT DISTINCT FROM r.station_id) AND
                    (weather_data.date IS NOT DISTINCT FROM r.date) AND
                    (weather_data.value IS NOT DISTINCT FROM r.value) 
                """
                )
        
        process_insert = PostgresOperator(
            task_id = "process_insert",
            postgres_conn_id = "postgres_default",
            sql = """       
                INSERT INTO weather_data AS w
                SELECT * FROM recent_insert_filtered 
                ON CONFLICT( measured, station_id, date) DO 
                UPDATE SET value = EXCLUDED.value
                """
                )
        process_update = PostgresOperator(
            task_id = "process_update",
            postgres_conn_id = "postgres_default",
            sql = """       
                INSERT INTO weather_data AS w
                SELECT * FROM recent_update_filtered 
                ON CONFLICT( measured, station_id, date) DO 
                UPDATE SET value = EXCLUDED.value
                """
                )
        
                #filter_quality_check = PostgresOperator(
            #task_id = "filter_quality_check",
            #postgres_conn_id = "postgres_default",
            #sql = """
            #"""
            #)
        
    
download_diff_weather_task >> [stage_recent_insert, stage_recent_update, stage_recent_delete]
stage_recent_insert >> filter_insert
stage_recent_delete >> filter_delete >> process_delete
stage_recent_update >> filter_update >> process_update
stage_recent_insert >> filter_insert >> process_insert
process_delete >> process_update >> process_insert


