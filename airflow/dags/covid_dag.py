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


import sql_covid_counties

args = {
    'owner': 'Airflow',
    'schedule_interval' : '@once',
    'start_date' : datetime(2021,5,2),
    'max_active_runs' : 1,

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
        self.url_dir = url_dir
        self.url_file = url_file
        self.out_dir = out_dir
        
    def execute(self, context):
        url = urljoin(self.url_dir, self.url_file)

        self.log.info(f'DOWNLOAD FROM {url}')
        out = os.path.join(self.out_dir, self.url_file) 
        urllib.request.urlretrieve(url, out)
        #ti.xcom_push(key='weather_url', value=url)
        #return url
    

      
with DAG(
    dag_id='covid_dag_pouet',
    default_args=args,
    start_date= datetime(2021,5,2), #days_ago(2), #datetime.datetime.now(), #days_ago(2),
    schedule_interval = '@once',
    tags=['covid'],
) as dag:
                                                           
        
        download_nyt_task = download_fromweb(task_id = "download_nyt")

        create_recent_per_county_table = PostgresOperator(
            task_id="create_recent_per_county_table",
            postgres_conn_id="postgres_default",
            sql = sql_covid_counties.q_create_recent_per_county_table
            ,
            )
        
        load_recent_per_county_table = PostgresOperator(
            task_id = "load_recent_per_county_table",
            postgres_conn_id = "postgres_default",
            sql = sql_covid_counties.q_load_recent_per_county_table,
            )
        # add location_id key to new data
        add_location_id = PostgresOperator(
            task_id = "add_location_id",
            postgres_conn_id = "postgres_default",
            sql =  sql_covid_counties.q_add_location_id
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
            sql =  sql_covid_counties.q_retrieve_past_data
            )   
        
        # compute daily deaths and cases from cumulated values :
        # - partition by location_id and order by date
        # - compute previous cases and deaths by "shifting" the case and deaths column
        # - compute daily data : cumulated_current - cumulated_prev
        compute_daily_stats = PostgresOperator(
            task_id = "compute_daily_stats",
            postgres_conn_id = "postgres_default",
            sql = sql_covid_counties.q_compute_daily_stats
                )
                
                
        filter_date = PostgresOperator(
            task_id = "filter_date",
            postgres_conn_id = "postgres_default",
            sql = sql_covid_counties.q_filter_date
            )
        update_last_date = PostgresOperator(
            task_id = "update_last_date",
            postgres_conn_id = "postgres_default",
            sql = sql_covid_counties.q_update_last_date
            )
        append_full_per_county = PostgresOperator(
            task_id = "append_full_per_county",
            postgres_conn_id = "postgres_default",
            sql = sql_covid_counties.q_append_full_per_county
            )
        
        drop_recent_per_county_table = PostgresOperator(
            task_id = "drop_recent_per_county_table",
            postgres_conn_id = "postgres_default",
            sql = sql_covid_counties.q_drop_recent_per_county_table
            )
        drop_recent_per_county_table_final = PostgresOperator(
            task_id = "drop_recent_per_county_table_final",
            postgres_conn_id = "postgres_default",
            sql = sql_covid_counties.q_drop_recent_per_county_table
            )
    
download_nyt_task >> drop_recent_per_county_table >> create_recent_per_county_table >> load_recent_per_county_table  >> add_location_id >> retrieve_past_data >> compute_daily_stats >> filter_date >> append_full_per_county >> update_last_date >> drop_recent_per_county_table_final
