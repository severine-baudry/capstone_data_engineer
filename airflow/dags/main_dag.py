from airflow.models import DAG
from airflow.operators.subdag import SubDagOperator

from datetime import datetime

from covid_dag import covid_subdag
from weather_dag import daily_weather_subdag

args = {
    'owner': 'Airflow',
    'schedule_interval' : '@once',
    'start_date' : datetime(2021,5,2),
    'max_active_runs' : 1,

}

PARENT_DAG_NAME = "main_dag"
SUBDAG_COUNTY_NAME = "covid_county_subdag"
SUBDAG_WEATHER_NAME = "weather_subdag"

with DAG(
    dag_id = PARENT_DAG_NAME,
    default_args = args,
    start_date= datetime(2021,5,2), #days_ago(2), #datetime.datetime.now(), #days_ago(2),
    schedule_interval = '@once',
    tags = ["covid"],
    ) as dag :
    
    sub_dag_covid_county = SubDagOperator(
        subdag=covid_subdag(PARENT_DAG_NAME,SUBDAG_COUNTY_NAME, args),
        task_id=SUBDAG_COUNTY_NAME
        )    
    sub_dag_weather = SubDagOperator(
        subdag=daily_weather_subdag(PARENT_DAG_NAME,SUBDAG_WEATHER_NAME, args),
        task_id=SUBDAG_WEATHER_NAME
        )
   
    
