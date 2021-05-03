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
    'schedule_interval' : '@daily',

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
        self.log.info( f"AVANT HIER {day_1}" )
        for i in range(1, self.max_days_depth):
            begin = ds_add(yesterday, -i)
            url_file = self.url_prefix +f"{ ds_format(begin, '%Y-%m-%d', '%Y%m%d')}_to_{ ds_format(yesterday, '%Y-%m-%d', '%Y%m%d')}.tar.gz"
            out = os.path.join(self.out_dir, url_file)
            if os.path.isfile(out) :
                self.log.info(f"{out} has been donwloaded !!")
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
                self.log.info(f"{out} does not exists ")
        else :
            self.log.error(f"downloading diff weather files failed : try to increase {self.max_days_depth}")
            raise(Exception( "downloading diff weather files failed") )

with DAG(
    dag_id='daily_weather',
    default_args=args,
    schedule_interval='@daily',
    start_date= datetime(2021,5,2), #days_ago(2), #datetime.datetime.now(), #days_ago(2),
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
            CREATE TABLE IF NOT EXISTS recent_insert (LIKE weather);
            COPY recent_insert FROM '"""  +
            "{{ti.xcom_pull(key='weather_diff_dir')}}"  + 
            """/insert.csv' WITH CSV HEADER;            
            """            
            )
#            SELECT {{ params.insert_file}};
#            SELECT {{ti.xcom_pull( key = \'weather_diff_dir\', task_ids= [\'download_diff_weather\'])}};

#            params = { "insert_file" : os.path.join( "{{ ti.xcom_pull(task_ids= ['download_diff_weather'], key = 'weather_diff_dir')}}", "insert.csv") }
#              params = { "insert_file" : "'{{ti.xcom_pull( key = \'weather_diff_dir\', task_ids= [\'download_diff_weather\'])}}'" }
#            params = { "insert_file" : {{xcom_pull( key = 'weather_diff_dir', task_ids= ['download_diff_weather'])}} }
#             params = { "insert_file" : "toto"}
#             "{{ ti.xcom_pull( key = 'weather_diff_dir', task_ids= ['download_diff_weather']) }} "  + "'";
      
        #filter_quality_check = PostgresOperator(
            #task_id = "filter_quality_check",
            #postgres_conn_id = "postgres_default",
            #sql = """
            #"""
            #)
        
    
download_diff_weather_task >> stage_recent_insert
