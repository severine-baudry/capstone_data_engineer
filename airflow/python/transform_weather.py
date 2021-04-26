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
from collections import OrderedDict

log_marker= "BLABLABLA-- "

def my_log(logger, msg):
    logger.warning( log_marker + msg)
    
def process_delete(ref_table, delete_table, log):
    '''
    remove all rows in delete_table from ref_table
    '''
    ref = ref_table.alias("ref").join( delete_table.alias("new"), 
                how = "left_anti",
                on = col("ref.station_id").eqNullSafe("new.station_id") &
                    col("ref.date").eqNullSafe("new.date") &
                    col("ref.measured").eqNullSafe("new.measured") &
                    col("ref.value").eqNullSafe("new.value") 
                    )
    return ref
                           
def upsert(ref_table, new_table, log):
    '''
    update or insert ref_table with rows from new_table
    '''
    #keep only rows of ref not in new_table
    my_log(log, f"UPSERT REF COLUMNS : {ref_table.columns}")
    my_log(log, f"NEW TABLE : {new_table.columns}")
    ref = ref_table.alias("ref").join( new_table.alias("new"), 
                    how = "left_anti",
                    on = col("ref.station_id").eqNullSafe("new.station_id") &
                    col("ref.date").eqNullSafe("new.date") &
                    col("ref.measured").eqNullSafe("new.measured")
                    )
    my_log(log, f"UPSERT AFTER ANTIJOIN : {ref.columns}")
    # add new rows                       )
    ref = ref.union(new_table).distinct()
    return ref




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
    
    weather_schema = "station_id string, date string, measured string, value string, measurement_flag string, quality_flag string, source_flag string, hour string"
    
    # load past weather table
    path_weather = sys.argv[3]
    db_weather = spark.read.parquet(path_weather)
    my_log(log, f"PAST_WEATHER : {path_weather} {db_weather.count()}")

    # load tables
    l_table_operations = OrderedDict()
    l_op = ["delete", "insert", "update"]
    for op in l_op :
        try :
            csv = os.path.join(path,op + ".csv")
            table = spark.read.csv( csv,  
                                schema = weather_schema)\
                                .filter(col("quality_flag").isNull())\
                                .select( * (db_weather.columns ) )
        except Exception as e:
            log.warning( f"No table {csv}" )
        else :
            l_table_operations[op] = table
            my_log(log, f"TABLE_SIZE_INIT {op} : {table.count()}")
 
    # join with stations table, to keep only measurements in the relevant stations
    path_stations = sys.argv[2]    
    my_log(log, f"READ_STATIONS : {path_stations}")
    map_locations_stations = spark.read.parquet(path_stations)

    for op, table in l_table_operations.items():
        l_table_operations[op] = table.join(map_locations_stations, ["station_id", "measured"])\
            .select(*(table.columns ))
        my_log(log, f"TABLE_SIZE_RELEVANT_STATIONS {op} : {l_table_operations[op].count()}")
            
 
        
    # process deletions
    db_weather = process_delete(db_weather, l_table_operations["delete"], log )
    my_log(log, f"AFTER DELETION : {db_weather.count()}\nSCHEMA : {db_weather.columns}")
    # process insertions
    my_log(log, "PROCESS_INSERTIONS")
    db_weather = upsert(db_weather, l_table_operations["insert"] , log)
    my_log(log, f"AFTER INSERTION : {db_weather.count()}\nSCHEMA : {db_weather.columns}")
    # process updates
    my_log(log, "PROCESS_UPDATES")
    db_weather = upsert(db_weather, l_table_operations["update"], log )
    my_log(log, f"AFTER UPDATE : {db_weather.count()}")
   
    
    #db_weather.write.format("parquet").mode("append").save(path_weather)
   
   
    
  
