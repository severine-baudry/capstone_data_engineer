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

import sql.weather_queries as sq
import os
from datetime import datetime, timedelta


args = {
    'owner': 'Airflow',
}


def daily_weather_subdag(parent_dag_id, child_dag_id, args):      

    with DAG(
        dag_id=f'{parent_dag_id}.{child_dag_id}',
        default_args=args,
        #schedule_interval= '@once', #'@monthly', # for testing #  @daily',
        #start_date= datetime(2021,5,2), #days_ago(2), #datetime.datetime.now(), #days_ago(2),
    #    end_date= datetime(2021,5,2), #days_ago(2), #datetime.datetime.now(), #days_ago(2),
       # max_active_runs = 1,
        #tags=['covid'],
        ) as dag:
        
            #download_diff_weather_task = download_diff_weather(task_id = "download_diff_weather")
            download_diff_weather_task = dummy_download(task_id = "download_diff_weather")
            # jinja + xcom + task parameters + sql string is uber relou
            stage_recent_insert = PostgresOperator(
                task_id = "stage_recent_insert",
                postgres_conn_id = "postgres_default",
                sql= sq.q_create_stage__weather_table,
                params = { "stage_table" : "recent_insert",
                        "stage_file" : "insert.csv"
                        }
                )
            stage_recent_delete = PostgresOperator(
                task_id = "stage_recent_delete",
                postgres_conn_id = "postgres_default",
                sql = sq.q_create_stage__weather_table,
                params = { "stage_table" : "recent_delete",
                        "stage_file" : "delete.csv"
                        }
                )
            stage_recent_update = PostgresOperator(
                task_id = "stage_recent_update",
                postgres_conn_id = "postgres_default",
                sql = sq.q_create_stage__weather_table,
                params = { "stage_table" : "recent_update",
                        "stage_file" : "update.csv"
                        }          
                )
            
            filter_insert = PostgresOperator(
                task_id = "filter_insert",
                postgres_conn_id = "postgres_default",
                sql = sq.q_filter_weather,
                params= {"full_table" : "recent_insert", 
                        "filtered_table" : "recent_insert_filtered",
                        "selected_stations" : "weatherelem_station"}
                )
            
            filter_delete = PostgresOperator(
                task_id = "filter_delete",
                postgres_conn_id = "postgres_default",
                sql = sq.q_filter_weather,
                params= {"full_table" : "recent_delete", 
                        "filtered_table" : "recent_delete_filtered",
                        "selected_stations" : "weatherelem_station"}
                )
            filter_update = PostgresOperator(
                task_id = "filter_update",
                postgres_conn_id = "postgres_default",
                sql = sq.q_filter_weather,
                params= {"full_table" : "recent_update", 
                        "filtered_table" : "recent_update_filtered",
                        "selected_stations" : "weatherelem_station"}
                )
            
            process_delete = PostgresOperator(
                task_id = "process_delete",
                postgres_conn_id = "postgres_default",
                sql = sq.q_process_delete_weather
                    )
            
            process_insert = PostgresOperator(
                task_id = "process_insert",
                postgres_conn_id = "postgres_default",
                sql = sq.q_process_insert_weather
                    )
            process_update = PostgresOperator(
                task_id = "process_update",
                postgres_conn_id = "postgres_default",
                sql = sq.q_process_update_weather
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
            
            return dag
        

