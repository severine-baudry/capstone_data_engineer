import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import MapType, StringType
from operator import add

import sys
from random import random
import logging
import os



if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("process_weather")\
        .getOrCreate()
    
    #log = logging.getLogger("py4j")  
    log = logging.getLogger("pyspark")  
    log.setLevel(logging.NOTSET)
    log.warning(f"lE DEBUT")
    log.warning(f"NB ARGS {len(sys.argv)}")
    if len(sys.argv) < 4:
        raise(Exception("not enough arguments : new_weather_file map_stations_db weather_db") )
    path = sys.argv[1]
    log.warning(f"READ DIFF WEATHER : {path}")
    weather = spark.read.csv(path,  
                         schema = "station_id string, date string, measured string, value string, measurement_flag string, quality_flag string, source_flag string, hour string")
    path_stations = sys.argv[2]
    log.warning(f"READ : {path_stations}")
    map_locations_stations = spark.read.parquet(path_stations)
    nb_new_weather = weather.count()
    log.warning(f"NB NEW WEATHER MEASURES : {nb_new_weather}")
    join_weather = weather.join(map_locations_stations, ["station_id", "measured"])
    log.warning(f"NEW RECORS : {join_weather.count()}")
    path_weather = sys.argv[3]
    db_weather = spark.read.parquet(path_weather)
    log.warning(f"READ : {path_weather}")
    selected_weather = join_weather.select( *db_weather.columns)
    log.warning(f"NEW RECORDS SELECTED : {selected_weather.count()}")
    schema = selected_weather.schema
    log.warning(f"SCHEMA : {schema}")
    selected_weather.write.format("parquet").mode("append").save(path_weather)
   
   
    
  
