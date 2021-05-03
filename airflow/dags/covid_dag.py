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
    

class download_diff_weather(BaseOperator):
    #template_fields = ["url_prefix"]
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
        self.log.info( f"I AM {self.__class_.__name__}" )
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
                
                context["task_instance"].xcom_push(key = "weather_diff_dir", value = untar_dir)
                break
        else :
            self.log.error(f"downloading diff weather files failed : try to increase {self.max_days_depth}")
            raise(Exception( "downloading diff weather files failed") )
     
with DAG(
    dag_id='covid_dag_pouet',
    default_args=args,
    schedule_interval='@daily',
    start_date= datetime(2021,5,2), #days_ago(2), #datetime.datetime.now(), #days_ago(2),
    tags=['covid'],
) as dag:
    
        #download_diff_weather_task = download_diff_weather(task_id = "download_diff_weather")
 #       download_diff_weather_task = dummy_download(task_id = "download_diff_weather")
                                                       
        
        download_nyt_task = download_fromweb(task_id = "download_nyt")
         
        #process_weather = SparkSubmitOperator(
            #application="/home/user/CODE/BIG_DATA/CAPSTONE_PROJECT/covid-analysis/airflow/python/transform_weather.py", 
            #task_id="process_weather",
            #conn_id = "spark_default",
            #application_args = [ "{{ ti.xcom_pull( task_ids = 'download_diff_weather', key = 'weather_diff_dir') }}", 
                                #"/home/user/CODE/BIG_DATA/CAPSTONE_PROJECT/covid-analysis/OUT_DATA/map_locations_stations",
                                #"/home/user/CODE/BIG_DATA/CAPSTONE_PROJECT/covid-analysis/OUT_DATA/weather_data"]

        #add_weather_task =  add_weather(
                #task_id = "add_weather",
                #path_patch_weather = "/tmp/superghcnd_diff_20210420_to_20210421.tar.gz",
                #path_patch_weather = '/tmp/superghcnd_diff_20210420_to_{{ ds_nodash }}.tar.gz',
                #db_stations = "/home/user/CODE/BIG_DATA/CAPSTONE_PROJECT/covid-analysis/OUT_DATA/map_locations_stations",
                #db_weather = "/home/user/CODE/BIG_DATA/CAPSTONE_PROJECT/covid-analysis/OUT_DATA/weather_data"
                #)

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
                SELECT  most_recent_per_location.location_id, most_recent_per_location.state, most_recent_per_location.county, most_recent_per_location.fips, most_recent_per_location.date, most_recent_per_location.cases, most_recent_per_location.deaths 
                FROM most_recent_per_location JOIN min_recent 
                ON most_recent_per_location.location_id = min_recent.location_id
                WHERE most_recent_per_location.date < min_recent.date;        
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
                SET last = most_recent_per_location.date
                FROM most_recent_per_location 
                WHERE most_recent_per_location.location_id = recent_per_county.location_id;
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
                CREATE TABLE new_recent AS

                WITH tmp0 AS
                (
                SELECT * FROM most_recent_per_location UNION SELECT date, location_id, fips, county, state, deaths, cases FROM recent_per_county
                ),
                tmp1 AS            
                (
                    SELECT *, rank() OVER (PARTITION BY location_id ORDER BY date DESC)
                    FROM tmp0
                )
                SELECT date, location_id, fips, county, state, deaths, cases
                FROM tmp1
                WHERE rank =1;
                
                DROP TABLE most_recent_per_location;
                ALTER TABLE new_recent RENAME TO most_recent_per_location;
            
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
        drop_recent_per_county_table_final = PostgresOperator(
            task_id = "drop_recent_per_county_table_final",
            postgres_conn_id = "postgres_default",
            sql = """
            DROP TABLE IF EXISTS recent_per_county;
            """
            )
       
     

#download_diff_weather_task >> process_weather
#download_nyt_task >> process_counties_task

download_nyt_task >> drop_recent_per_county_table >> create_recent_per_county_table >> load_recent_per_county_table  >> add_location_id >> retrieve_past_data >> compute_daily_stats >> filter_date >> append_full_per_county >> update_last_date >> drop_recent_per_county_table_final
