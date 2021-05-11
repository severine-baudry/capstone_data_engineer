from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from airflow.macros import ds_add, ds_format
from airflow.operators.python import get_current_context

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
        context["task_instance"].xcom_push(key = "last_cdc_date", value = str_date)
                        

def covid_per_popgroup_subdag(parent_dag_id, child_dag_id, args):      
        with DAG(
            dag_id=f'{parent_dag_id}.{child_dag_id}',
            default_args=args,
            #start_date= datetime(2021,5,2), #days_ago(2), #datetime.datetime.now(), #days_ago(2),
            #schedule_interval = '@once',
            #tags=['covid'],
        ) as dag:
    
        
            last_date_popgroup_task = get_last_date_popgroup(
                task_id = "last_date_popgroup_task"
                )
            
            current_dir = os.path.dirname(os.path.abspath(__file__))
            root_dir = os.path.dirname(current_dir)
            download_recent_cdc_task = SparkSubmitOperator(
                task_id = "download_recent_cdc_task",
                conn_id = "spark_default",
                application = os.path.join(root_dir, "python", "stage_recent_cdc.py"),
                application_args = ["--apptoken", Variable.get("socrata_apptoken"), 
                                    "--last_date", "{{ti.xcom_pull( task_ids = 'last_date_popgroup_task', key = 'last_cdc_date')}}",
                                    ],
                )
                
            insert_covid_pergroup_task = PostgresOperator(
                task_id = "insert_covid_pergroup_task",
                postgres_conn_id = "postgres_default",
                sql = """
                    INSERT INTO covid_per_popgroup(cdc_case_earliest_dt, sex_id, age_group_id, race_ethnicity_id, count)
                    SELECT cdc_case_earliest_dt, sex_id, age_group_id, race_ethnicity_id, count
                    FROM recent_cdc AS n
                    JOIN  dim_age_group AS a ON a.age_group = n.age_group
                    JOIN dim_sex AS s ON s.sex = n.sex
                    JOIN dim_race_ethnicity AS e ON e.race = n.race_ethnicity_combined
                    ;
                """
                )
            last_date_popgroup_task >>  download_recent_cdc_task >> insert_covid_pergroup_task
            return dag
