from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import os
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

class download_fromweb(BaseOperator):
    template_fields = ["url_file"]
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 url_dir = "https://github.com/nytimes/covid-19-data/raw/master/",
                 url_file ="us-counties-recent.csv",
                 out_dir="/tmp/",
                 *args, **kwargs):

        super(download_fromweb, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.url_dir = url_dir
        self.url_file = url_file
        self.out_dir = out_dir
        
    def execute(self, context):
        url = urljoin(self.url_dir, self.url_file)

        self.log.info(f'DOWNLOAD FROM {url}')
        out = os.path.join(self.out_dir, self.url_file) 
        urllib.request.urlretrieve(url, out)
    
      
with DAG(
    dag_id='covid_dag_pouet',
    default_args=args,
    #schedule_interval=None,
    start_date=days_ago(2),
    tags=['covid'],
) as dag:
        if 0:
            download_nyt_task = download_fromweb(task_id = "download_nyt")
        download_weather_task = download_fromweb(task_id = "download_weather",
                                    url_dir = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/superghcnd/",
                                    url_file = "superghcnd_diff_{{ yesterday_ds_nodash }}_to_{{ ds_nodash }}.tar.gz")
#                                   url_file = "superghcnd_diff_20210420_to_20210421.tar.gz")
        
        process_weather = SparkSubmitOperator(
            application="/home/user/airflow/python/transform_weather.py", 
            task_id="process_weather",
            conn_id = "spark_default",
            application_args = ["/tmp/superghcnd_diff_{{ yesterday_ds_nodash }}_to_{{ ds_nodash }}.tar.gz", 
                                "/home/user/CODE/BIG_DATA/CAPSTONE_PROJECT/covid-analysis/OUT_DATA/map_locations_stations",
                                "/home/user/CODE/BIG_DATA/CAPSTONE_PROJECT/covid-analysis/OUT_DATA/weather_data"]
    
        #add_weather_task =  add_weather(
                #task_id = "add_weather",
##                path_patch_weather = "/tmp/superghcnd_diff_20210420_to_20210421.tar.gz",
                #path_patch_weather = '/tmp/superghcnd_diff_20210420_to_{{ ds_nodash }}.tar.gz',
                #db_stations = "/home/user/CODE/BIG_DATA/CAPSTONE_PROJECT/covid-analysis/OUT_DATA/map_locations_stations",
                #db_weather = "/home/user/CODE/BIG_DATA/CAPSTONE_PROJECT/covid-analysis/OUT_DATA/weather_data"
    )

        
    #download_nyt = SparkSubmitOperator(
        #application="/home/user/CODE/BIG_DATA/download_nyt.py", 
        #task_id="download_nyt",
        #conn_id = "spark_default"
    #)

download_weather_task >> process_weather
