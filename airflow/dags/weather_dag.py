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
import tarfile

import urllib
import requests
from urllib.error import *
from urllib.parse import urljoin

import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging


args = {
    'owner': 'Airflow',
}

class download_diff_weather(BaseOperator):
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 url_dir = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/superghcnd/",
                 url_prefix ="superghcnd_diff_",
                 out_dir="/tmp/",
                 max_days_depth = 5,
                 *args, **kwargs):

        super(download_diff_weather, self).__init__(*args, **kwargs)
        # Map params here
        self.url_dir = url_dir
        self.url_prefix = url_prefix
        self.out_dir = out_dir
        self.max_days_depth = max_days_depth
        
    def execute(self, context):
        self.log.info( f"I AM {self.__class__.__name__}" )
        yesterday = context["yesterday_ds"]
        day_1 = ds_add(yesterday, -1)
        self.log.info( f"AVANT HIER {day_1}" )
        self.log.info( f"MAX DAYS DEPTH {self.max_days_depth}" )
        for i in range(1, self.max_days_depth):
            begin = ds_add(yesterday, -i)
            url_file = self.url_prefix +f"{ ds_format(begin, '%Y-%m-%d', '%Y%m%d')}_to_{ ds_format(yesterday, '%Y-%m-%d', '%Y%m%d')}.tar.gz"
            self.log.info( f"URL : {url_file}")
            url = urljoin(self.url_dir, url_file)
            out = os.path.join(self.out_dir, url_file)
            try :
                urllib.request.urlretrieve(url, out)
            except Exception as e :
                self.log.info(f"cannot download {url_file} : {e}")
            else :
                self.log.info(f"Download {url_file} :  success !!")
                context["task_instance"].xcom_push(key = "first_date", value = begin)
                context["task_instance"].xcom_push(key = "last_date", value = yesterday)
                
                tar = tarfile.open(out, "r:*")
                tar_root = os.path.commonprefix(tar.getnames())
                untar_dir = os.path.join("/tmp/", tar_root)
                self.log.info( f"TAR DIR : {untar_dir}" )
                tar.extractall( "/tmp/")
                tar.close()
                # enable read for all users for the untar files
                os.chmod(untar_dir, 0o755)
                for t in os.listdir(untar_dir):
                    path = os.path.join(untar_dir, t)
                    os.chmod(path, 0o644)
                context["task_instance"].xcom_push(key = "weather_diff_dir", value = untar_dir)
                break
        else :
            self.log.error(f"downloading diff weather files failed : try to increase {self.max_days_depth}")
            raise(Exception( "downloading diff weather files failed") )

class dummy_download( BaseOperator):   
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 url_dir = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/superghcnd/",
                 url_prefix ="superghcnd_diff_",
                 out_dir="/tmp/",
                 max_days_depth = 5,
                 *args, **kwargs):

        super(dummy_download, self).__init__(*args, **kwargs)
        # Map params here
        self.url_dir = url_dir
        self.url_prefix = url_prefix
        self.out_dir = out_dir
        self.max_days_depth = max_days_depth
        
    def execute(self, context):
        self.log.info( f"I AM {self.__class__.__name__}" )
        yesterday = context["yesterday_ds"]
        day_1 = ds_add(yesterday, -1)
        end = ds_add(yesterday, 0)
        self.log.info( f"yesterday {yesterday}" )
        self.log.info( f"END pouet {end}" )
        for i in range(1, self.max_days_depth):
            begin = ds_add(end, -i)
            url_file = self.url_prefix +f"{ ds_format(begin, '%Y-%m-%d', '%Y%m%d')}_to_{ ds_format(end, '%Y-%m-%d', '%Y%m%d')}.tar.gz"
            out = os.path.join(self.out_dir, url_file)
            if os.path.isfile(out) :
                self.log.info(f"{out} has been donwloaded !!")
                context["task_instance"].xcom_push(key = "first_date", value = begin)
                context["task_instance"].xcom_push(key = "last_date", value = end)
                tar = tarfile.open(out, "r:*")
                tar_root = os.path.commonprefix(tar.getnames())
                untar_dir = os.path.join("/tmp/", tar_root)
                self.log.info( f"TAR DIR : {untar_dir}" )
                tar.extractall( "/tmp/")
                tar.close()
                # enable read for all users for the untar files
                os.chmod(untar_dir, 0o755)
                for t in os.listdir(untar_dir):
                    path = os.path.join(untar_dir, t)
                    os.chmod(path, 0o644)
               
                context["task_instance"].xcom_push(key = "weather_diff_dir", value = untar_dir)
                break
            else :
                self.log.info(f"{out} does not exists ")
        else :
            self.log.error(f"downloading diff weather files failed : try to increase {self.max_days_depth}")
            raise(Exception( "downloading diff weather files failed") )

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
            
sql_filter_v1 = """
            DROP TABLE IF EXISTS {{params.filtered_table}};
            CREATE TABLE {{params.filtered_table}} AS
            WITH temp AS (
                SELECT * FROM {{params.full_table}}
                WHERE quality_flag IS NULL
                )
            SELECT * 
            FROM temp JOIN {{params.selected_stations}}  AS sel
            ON sel.location_id = temp.location_id AND sel.date = temp.date
            
            """
sql_filter_v0 = """
            SELECT * FROM {{params.table}}
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
            sql="""
            DROP TABLE IF EXISTS recent_insert;
            CREATE TABLE IF NOT EXISTS recent_insert(
                station_id varchar(12),
                date varchar(8),
                measured varchar(4),
                value int, 
                measurement_flag varchar(1), 
                quality_flag varchar(1), 
                source_flag varchar(1), 
                hour varchar(6)
            );
            
            COPY recent_insert FROM '"""  +
            "{{ti.xcom_pull(key='weather_diff_dir')}}"  + 
            """/insert.csv' WITH CSV HEADER; 
            """            
            )
        stage_recent_delete = PostgresOperator(
            task_id = "stage_recent_delete",
            postgres_conn_id = "postgres_default",
            sql="""
            DROP TABLE IF EXISTS recent_delete;
            CREATE TABLE IF NOT EXISTS recent_delete(
                station_id varchar(12),
                date varchar(8),
                measured varchar(4),
                value int, 
                measurement_flag varchar(1), 
                quality_flag varchar(1), 
                source_flag varchar(1), 
                hour varchar(6)
            );
            COPY recent_delete FROM '"""  +
            "{{ti.xcom_pull(key='weather_diff_dir')}}"  + 
            """/delete.csv' WITH CSV HEADER;            
            """            
            )
        stage_recent_update = PostgresOperator(
            task_id = "stage_recent_update",
            postgres_conn_id = "postgres_default",
            sql="""
            DROP TABLE IF EXISTS recent_update;
            CREATE TABLE IF NOT EXISTS recent_update(
                station_id varchar(12),
                date varchar(8),
                measured varchar(4),
                value int, 
                measurement_flag varchar(1), 
                quality_flag varchar(1), 
                source_flag varchar(1), 
                hour varchar(6)
            );
            COPY recent_update FROM '"""  +
            "{{ti.xcom_pull(key='weather_diff_dir')}}"  + 
            """/update.csv' WITH CSV HEADER;            
            """            
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


