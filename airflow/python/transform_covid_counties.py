import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import MapType, StringType
from operator import add
from airflow.models import Variable
from airflow.macros import ds_add, ds_format

import sys
from random import random
import logging
import os
from collections import OrderedDict
from datetime import datetime, timedelta

log_marker= "BLABLABLA-- "

def my_log(logger, msg):
    logger.warning( log_marker + msg)
    
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("process_covid_geography")\
        .getOrCreate()
    
    #log = logging.getLogger("py4j")  
    log = logging.getLogger("pyspark")  
    log.setLevel(logging.NOTSET)
    log.warning(f"lE DEBUT")
    my_log(log, f"NB ARGS {len(sys.argv)}")
    #if len(sys.argv) < 4:
        #raise(Exception("not enough arguments : new_weather_file map_stations_db weather_db") )
    #diff_weather_file = sys.argv[1]
    #my_log(log, f"READ DIFF WEATHER : {diff_weather_file}")
    
    try :
        
        last_date_str = Variable.get("most_recent_date_couties")
        last_date = datetime.strptime(last_date_str, "%Y-%m-%d")

    except Exception as e :
        my_log(log, f"{e}")
        df_covid = spark.read.parquet(sys.argv[2])
        # compute last date as the max of dates currently in table
        last_date =  df_covid.agg({"date" : "max" }).collect()[0]["max(date)"]
        
    my_log(log, f"LAST DATE: {datetime.strftime(last_date, '%Y-%m-%d')} TYPE : {type(last_date)}" )

    df_new_covid = spark.read.option("header", True).csv(sys.argv[1])
    df_new_covid.withColumn("date", F.to_date("date"))
    
    df_new_covid = df_new_covid.where(col("date")> last_date)
    
    #df_new_covid.where(col("date")
                       
    #new_min_date = df_new_covid.agg({"date" : "min" }).collect()[0]["min(date)"]
 
