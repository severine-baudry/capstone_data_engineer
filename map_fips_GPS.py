#!/usr/bin/env python
# coding: utf-8

# mapping of covid locations (fips + GPS) to the closest weather station

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

import logging
import argparse

from create_spark_session import *


# parse raw stations into columns
@udf(MapType( StringType(), StringType()))
def ParseStationsUDF(line):
    """
    parsing of weather stations (fixed length fields)
    """
    return{
        "station_id": line[0:11],
        "latitude" : line[13:20], 
        "longitude" : line[21:30], 
        "elevation" : line[31:38], 
        "state" : line[38:40], 
        "station_name" : line[41:]
        
    }

fields = OrderedDict( [
        ( "station_id" , "string"),
        ( "latitude" , "float"), 
        ("longitude" , "float"), 
        ("elevation" , "float"),
        ("state" , "string"), 
        ("station_name" , "string")
] )

#exprs = [ f"parsed['{field}'].cast({fld_type}) as {field}" for field, fld_type in fields.items() ]
exprs = [ f"CAST(parsed['{field}'] AS {fld_type}) AS {field}" for field, fld_type in fields.items() ]

def precompute_distance(l_ref : DataFrame) -> DataFrame:
    """
    precomputations required to evaluate distance between 2 GPS coordinates
    """
    l_ref = l_ref.withColumnRenamed("latitude", "latitude_degrees")
    l_ref = l_ref.withColumnRenamed( "longitude", "longitude_degrees") 
    @udf( FloatType())
    def degree_to_radian(x):
        return  x* np.pi / 180.
    l_ref = l_ref.withColumn("latitude", degree_to_radian("latitude_degrees") )
    l_ref = l_ref.withColumn("longitude", degree_to_radian("longitude_degrees") )
    l_ref = l_ref.withColumn("cos_latitude", F.cos("latitude") )  
    print(type(l_ref))
    return l_ref


def delta_coord(col1, col2):
    return F.pow( F.sin(0.5* (col1 - col2 ) ), 2 )

def haversine( lat1, long1, cos_lat1, lat2, long2, cos_lat2):
    ''' computation of angular distance between 2 locations given by GPS coordinates,
    using haversine formula.
    exact formulas (maybe overkill), taken from :
    https://www.movable-type.co.uk/scripts/latlong.html
    '''    
    delta_lat = delta_coord(lat1, lat2)
    delta_long = delta_coord(long1, long2)
    a = delta_lat + delta_long * cos_lat1 * cos_lat2
    return  F.atan2( F.sqrt(a), F.sqrt( 1.-a ) )

class MapCountiesToWeatherStations():
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        
        self.input_data_dir = config["PATH"]["HDFS_DOWNLOAD_DIR"]
        self.output_path =  config["PATH"]["STAGE_DATA_DIR"]

    
    def load_stations(self, selected_stations):
        """
        load weather stations, and filter to keep only relevant stations
        """
        
        # load all stations, with GPS location
        raw_stations = self.spark.read.csv( os.path.join(self.input_data_dir, "WEATHER", "ghcnd-stations.txt"))
        self.stations = raw_stations.withColumn("parsed", ParseStationsUDF("_c0")).selectExpr( *exprs)
        # filter all stations to keep only relevant stations
        df_stations = self.stations.join(selected_stations, ["station_id"])


        # some stations have identical latitude and longitude => keep only one within a GPS group
        w = Window.partitionBy("latitude", "longitude").orderBy("elevation", "station_id")
        self.df_stations_dedup = df_stations\
            .withColumn("rank_elevation", F.rank().over(w))\
            .where( col("rank_elevation") == 1)\
            .drop("rank_elevation")\
            .cache()

        w = Window.partitionBy("station_id").orderBy("measured")
        self.unique_stations = self.df_stations_dedup\
            .withColumn("dummy", F.rank().over(w) )\
            .where(col("dummy") == 1)\
            .drop("dummy")
        
    def find_closest_station(self, county_location):
        """
        for each county, find the closest weather station
        """
        # check that all GPS coordinates are defined
        county_location = county_location.where( ( ~ F.isnan("latitude") ) | (~ F.isnan("longitude")) )
    
        # precomputations to speed-up pairwise distance computation
        df_stations_precompute =precompute_distance(self.df_stations_dedup)
        county_location_precompute = precompute_distance(county_location)

        # check that precomputation do not yield NaN values
        df_stations_precompute.where(F.isnan("cos_latitude") ).show()

        sub_fips = county_location_precompute\
            .select("location_id", "fips", "county", "state", "latitude", "longitude", "cos_latitude")\
            .withColumnRenamed("latitude", "latitude_fips")\
            .withColumnRenamed("longitude", "longitude_fips")\
            .withColumnRenamed("cos_latitude", "cos_latitude_fips")

        sub_stations = df_stations_precompute\
            .select("station_id", "measured","latitude", "longitude", "cos_latitude")\
            .withColumnRenamed("latitude", "latitude_station")\
            .withColumnRenamed("longitude", "longitude_station")\
            .withColumnRenamed("cos_latitude", "cos_latitude_station")

        # all possible (weather station, county) pairs
        fips_cross_stations = sub_fips.crossJoin(sub_stations)

        # compute all pair-wise angles btw counties(FIPS) and weather stations
        dist_col = "angle"
        df_cross_distance = fips_cross_stations.withColumn(dist_col,                                  
        haversine(col("latitude_fips"), col("longitude_fips"),col("cos_latitude_fips"), 
                    col("latitude_station"), col("longitude_station"), col("cos_latitude_station")) )

        R_earth = 6371
        # for each measurement and FIPS, keep only the station with the smallest distance (e.g. smallest angle)
        # and compute distance from angle
        window = Window.partitionBy("measured", "location_id")
        df_min_distance = df_cross_distance.withColumn("min_angle", F.min(dist_col).over(window))\
            .filter( col(dist_col) == col("min_angle") )\
            .withColumn("distance", R_earth * col("angle"))


        # join with fips and stations DB to get all relevant data
        self.res_detailed = df_min_distance.alias("closest")\
            .join( county_location.alias("fips"), ["location_id"])\
            .join(self.unique_stations.alias("station"), "station_id")\
            
        self.res_detailed = self.res_detailed.select("closest.measured", "location_id", "fips.fips", "fips.county", col("fips.state").alias("fips_state"),
                    "station_id", "station_name", col("station.state").alias("station_state"),
                      "angle", "distance"        )


    def filter_outliers(self, max_distance = 600):
        """
        filter out mappings where the distance btw weather station and county is more than max_distance
        This can happen for counties with a very small population (e.g. in Alaska), or for weather elements
        that are not measured in some locations (e.g. snow in Pacific islands)
        
        """

        # remove mappings where distance from fips to station is more than 600 km
        self.map_county_stations = self.res_detailed.where( col("distance") < max_distance)\
            .select("location_id", "measured", "station_id", "distance")
    

    def process_weather(self, weather_2020):
        """
        process weather records to 
        """
        # read weather records for 2021; keep only records which passed quality check
        path = os.path.join(self.input_data_dir, "WEATHER", "2021.csv.gz")
        weather_2021 = self.spark.read.csv(path,  
                                schema = "station_id string, date string, measured string, value int, measurement_flag string, quality_flag string, source_flag string, hour string")\
                        .withColumn("date", F.to_date(col("date"), 'yyyyMMdd') )\
                        .filter( col("quality_flag").isNull())
                    
        # concatenate with previously loaded weather records from 2020            
        weather = weather_2020.select(weather_2021.columns)\
                                .union(weather_2021)
        
        # keep only records for selected stations
        stations_subset = self.map_county_stations\
            .select("measured", "station_id")\
            .distinct()

        self.selected_weather = weather.join( stations_subset, on = ["station_id", "measured"])\
                        .select("measured", "station_id", "date", "value")

        
    def write_data(self):
        """
        output a relevant subset of weather stations
        """
        
        # output the mapping between counties and weather stations
        out_path = os.path.join(self.output_path, "map_locations_stations")
        self.map_county_stations\
            .repartition("measured")\
            .write\
            .partitionBy("measured")\
            .format("parquet")\
            .mode("overwrite")\
            .save(out_path)

        # # Output selected stations
        col_stations = self.stations.columns
        selected_stations = self.stations.join(self.map_county_stations, on = ["station_id"])\
            .select(*col_stations)\
            .distinct()

        out_path = os.path.join(self.output_path, "weather_stations")
        selected_stations.repartitionByRange(5, "station_id")\
            .write.format("parquet").mode("overwrite").save(out_path)


        out_path = os.path.join(self.output_path, "weather_records")
        self.selected_weather.repartition("measured")\
            .repartitionByRange(5,"station_id")\
            .write\
            .partitionBy("measured")\
            .mode("overwrite")\
            .parquet(out_path)

    def process(self, selected_stations, county_location, weather_2020, max_distance = 600):
        self.load_stations(selected_stations)        
        self.find_closest_station(county_location)
        self.filter_outliers(max_distance)
        self.process_weather(weather_2020)
        self.write_data()

def main(configname = "capstone.cfg"):
    """
    main to test ETL for locations data
    """
    config = configparser.ConfigParser()
    config.read(configname)
    spark = create_spark_session()
    
    input_data_dir = config["PATH"]["HDFS_DOWNLOAD_DIR"]
    output_path =  config["PATH"]["STAGE_DATA_DIR"]
    
    # load data (in full script execution, these data have been previously loaded)
    path = os.path.join(output_path, "filtered_stations")
    selected_stations = spark.read.parquet(path)
    
    path = os.path.join(output_path, "nyt_locations_geography")
    county_location = spark.read.parquet(path)
    
    path = os.path.join(input_data_dir, "WEATHER", "2020.csv.gz")
    weather_2020 = spark.read.csv(path,  
            schema = "station_id string, date string, measured string, value int, measurement_flag string, quality_flag string, source_flag string, hour string")\
            .withColumn("date", F.to_date(col("date"), 'yyyyMMdd') )
                
                
    map_counties_stations = MapCountiesToWeatherStations(spark, config)
    
 
    map_counties_stations.process( selected_stations, county_location, weather_2020)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", default = "capstone.cfg")
    l_args = parser.parse_args()
    
    main(l_args.config)
