from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import BaseOperator, Variable
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

from sodapy import Socrata


args = {
    'owner': 'Airflow',
}

class download_cdc(BaseOperator):
    @apply_defaults
    def __init__(self,
                 url = "data.cdc.gov",
                 dataset_identifier = "vbim-akqf",
                 socrata_apptoken_var = "socrata_apptoken",
                 timeout = 100,
                 *args, **kwargs):
        super(download_cdc, self).__init__(*args, **kwargs)
        self.url = url
        self.dataset_identifier = dataset_identifier
        self.socrata_apptoken_var = socrata_apptoken_var
        self.timeout = timeout
        
    def execute(self, context):
        self.log.info( f"I AM {self.__class__.__name__}" )
        apptoken = Variable.get(self.socrata_apptoken_var)
        client = Socrata(self.url,
                 apptoken,
                 timeout = self.timeout)
        self.log.info(f"socrata app token = {apptoken}")
        #str_min_date = datetime(2021, 4,1).isoformat() + ".000"
        str_min_date = context["task_instance"].xcom_pull(key='last_date')
        self.log.info(f"min date : {str_min_date}")
        cases_per_date = client.get(self.dataset_identifier,
                           group = "cdc_case_earliest_dt, sex, age_group, race_ethnicity_combined",
                           select = "cdc_case_earliest_dt, sex, age_group, race_ethnicity_combined, count(*)"
                           ,where = f"cdc_case_earliest_dt > '{str_min_date}'",
                           limit = 200000
                           )
        self.log.info(f"nb records : {len(cases_per_date)}")

class get_last_date_popgroup(BaseOperator):
    @apply_defaults
    def __init__(self,
                 postgres_conn_id = "postgres_default",
                 *args, **kwargs):
        super(get_last_date_popgroup, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
    def execute(self, context):
        connection = PostgresHook(postgres_conn_id = self.postgres_conn_id)
        results = connection.get_records("SELECT max(cdc_case_earliest_dt) FROM covid_per_popgroup")
        last_datetime = results[0][0]
        last_date = datetime(last_datetime.year, last_datetime.month, last_datetime.day)
        self.log.info(f"last_date : {last_date}, {type(last_date)}")
       
        #str_date = results[0][0].isoformat()
        str_date = last_date.isoformat()+".000"
        self.log.info(f"str_date : {str_date}, {type(str_date)}")
        context["task_instance"].xcom_push(key = "last_date", value = str_date)

with DAG(
    dag_id='covid_per_popgroup',
    default_args=args,
    schedule_interval= '@once', #'@monthly', # for testing #  @daily',
    start_date= datetime(2021,5,2), #days_ago(2), #datetime.datetime.now(), #days_ago(2),
#    end_date= datetime(2021,5,2), #days_ago(2), #datetime.datetime.now(), #days_ago(2),
    max_active_runs = 1,
    tags=['covid'],
    ) as dag:
        
        last_date_popgroup_task = get_last_date_popgroup(
            task_id = "last_date_popgroup_task"
            )
        download_cdc_task = download_cdc(
                task_id = "download_cdc_task")
    
last_date_popgroup_task >>  download_cdc_task   
