#!/usr/bin/env python
# coding: utf-8

# In[1]:


import configparser
import os
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession


config = configparser.ConfigParser()
config.read("redshift.cfg")
output_path =  config["PATH"]["STAGE_DATA_DIR"]



spark = SparkSession \
        .builder \
        .appName("s3_to_redshift")\
        .config("spark.driver.extraClassPath", "redshift-jdbc42-2.0.0.4.jar")\
        .config("spark.jars", "redshift-jdbc42-2.0.0.4.jar")\
        .getOrCreate()


class ParquetToRedshift():
    
    def __init__(self, configname = "redshift.cfg"):
        self.config = configparser.ConfigParser()
        self.config.read(configname)
        self.output_path =  self.config["PATH"]["STAGE_DATA_DIR"]
        rs = self.config["REDSHIFT"]
        self.url = f"jdbc:redshift://{rs['ENDPOINT']}:{rs['PORT']}/{rs['DB']}"
        self.user= rs['USER']
        self.password = rs['PASSWORD']

    def write_to_redshift(self, dataframe, table_name):
        dataframe.write\
            .format("jdbc")\
            .option("url", url)\
            .option("dbtable", table_name)\
            .option("user",user)    .option("password", password)    .save()

def parquet_to_redshift()

table_name = "nyt_locations_geography"
print(f"Reading {table_name}")
out_path = os.path.join(output_path,table_name )
nyt_geography = spark.read.parquet(out_path)


# Hyperasspain : redshift jdbc driver does not allow to write null values !!! 
# thus replace null values by empty string ''
nyt_geography    .fillna({"fips" : ""})    .write    .format("jdbc")    .option("url", url)    .option("dbtable", table_name)    .option("user",user)    .option("password", password)    .save()


# In[21]:


table_name = "map_locations_stations"
print(f"writing {table_name}")
out_path = os.path.join(output_path,table_name )
map_location_stations = spark.read.parquet(out_path)

map_location_stations.write    .format("jdbc")    .option("url", url)    .option("dbtable", table_name)    .option("user",user)    .option("password", password)    .save()


table_name = "weather_stations"
print(f"Writing {table_name}")
out_path = os.path.join(output_path,table_name )
selected_stations = spark.read.parquet(out_path)


selected_stations.write    .format("jdbc")    .option("url", url)    .option("dbtable", table_name)    .option("user",user)    .option("password", password)    .save()

table_name = "weather_records"
print(f"Writing {table_name}")
out_path = os.path.join(output_path,table_name )
selected_weather = spark.read.parquet(out_path)

selected_weather.write    .format("jdbc")    .option("url", url)    .option("dbtable", table_name)    .option("user",user)    .option("password", password)    .save()

table_name = "last_data_per_county"
print(f"Writing {table_name}")
out_path = os.path.join(output_path,table_name )
last_data = spark.read.parquet(out_path)
last_data.write    .format("jdbc")    .option("url", url)    .option("dbtable", table_name)    .option("user","postgres")    .option("password", "postgres")    .save()

table_name="covid_per_county"
print(f"Writing {table_name}")
out_path = os.path.join(output_path,table_name )
last_data = spark.read.parquet(out_path)
last_data.write    .format("jdbc")    .option("url", url)    .option("dbtable", table_name)    .option("user","postgres")    .option("password", "postgres")    .save()
