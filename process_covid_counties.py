#!/usr/bin/env python
# coding: utf-8


import configparser
import os
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import MapType, StringType, FloatType
from pyspark.sql import DataFrame, Window
from collections import OrderedDict
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import argparse
from create_spark_session import *


class CovidPerCounty():
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        
        self.output_path =  config["PATH"]["STAGE_DATA_DIR"]
        
    def filter_locations(self, covid_per_county, locations):
        """
        add location id to covid data, and remove rows which do not match a valid location id
        """
        self.df_covid_filter = covid_per_county.alias("covid").join( locations.alias("loc"),
                    (covid_per_county.fips.eqNullSafe(locations.fips) ) &
                    (covid_per_county.county.eqNullSafe(locations.county) ) & 
                    (covid_per_county.state.eqNullSafe( locations.state) ) )\
            .select("date", "location_id", "covid.fips", "covid.county", "covid.state", "deaths", "cases")                                             
        
    def daily_statistics(self):
        """
        compute daily cases and daily deaths, from cumulated cases and deaths values
        """
        
        # first add a column with the lag value (i.e. the value from the previous day)
        # then compute the difference btw current day and previous day values
        w = Window.partitionBy("location_id").orderBy("date")
        self.covid_daily = self.df_covid_filter\
            .withColumn("deaths_prev", F.lag("deaths", count = 1, default = 0).over(w) )\
            .withColumn("cases_prev", F.lag("cases", count = 1, default = 0).over(w) )\
            .withColumn("daily_deaths", col("deaths") - col("deaths_prev"))\
            .withColumn("daily_cases", col("cases") - col("cases_prev"))
    
    def last_cumulated_data(self):
        """
        extract last cumulated data for each location id, which will be needed to compute daily stats when new data are available
        """
        w = Window.partitionBy("location_id").orderBy( col("date").desc())
        columns = self.df_covid_filter.columns[:]
        self.last_data = self.df_covid_filter.withColumn("rank", F.rank().over(w))\
            .where("rank == 1")\
            .select(columns)


    
    def write_data(self):
        """
        write daily covid statistics
        """
        out_path = os.path.join(self.output_path, "covid_per_county")

        self.covid_daily.select("date", "location_id", "daily_cases", "daily_deaths")\
            .repartitionByRange(10, "location_id")\
            .write\
            .option("header","true")\
            .mode("overwrite")\
            .csv(out_path)

        out_path = os.path.join(self.output_path,"last_data_per_county" )
        self.last_data\
            .repartitionByRange(10, "location_id")\
            .write\
            .option("header","true")\
            .mode("overwrite")\
            .csv(out_path)
    
    def ETL(self, covid_per_county, locations):
        self.filter_locations(covid_per_county, locations)
        self.daily_statistics()
        self.last_cumulated_data()
        self.write_data()
        
        
def main(configname = "capstone.cfg"):
    """
    main to test ETL for covid per county
    """
    config = configparser.ConfigParser()
    config.read(configname)
    spark = create_spark_session()
    
    input_data_dir = config["PATH"]["HDFS_DOWNLOAD_DIR"]
    output_path =  config["PATH"]["STAGE_DATA_DIR"]
    
    # load data (in full script execution, these data have been previously loaded)
    nyt_covid = "us-counties.csv"
    path = os.path.join(input_data_dir, "COVID",nyt_covid)
    covid_data = spark.read.csv(path , header = True)\
        .withColumn("date", F.to_date("date"))\
        .withColumn("cases", col("cases").cast(T.IntegerType()) )\
        .withColumn("deaths", col("deaths").cast(T.IntegerType()) )

    path = os.path.join(output_path, "nyt_locations_geography")
    nyt_locations = spark.read.parquet(path)
    
    # process and outputs covid data
    covid_per_county = CovidPerCounty(spark, config)       
    covid_per_county.ETL(covid_data, nyt_locations)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", default = "capstone.cfg")
    l_args = parser.parse_args()
    
    main(l_args.config)


