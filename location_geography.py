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
from pyspark.sql.types import MapType, StringType
from collections import OrderedDict
import pandas as pd
import numpy as np
import logging
import argparse

from create_spark_session import *


@udf(MapType( StringType(), StringType()))
def ParseGazetteerUDF(line):
    l_str = line.split()
    l = len(l_str)
    l_headers = 10
    n_words = l - l_headers + 1
    county = " ".join( l_str[3:3+n_words] )

    return{
        "state": l_str[0],
        "county" : county ,        
        "fips" : l_str[1], 
        "latitude" : l_str[-2], 
        "longitude" : l_str[-1] 
    }



class CountiesGeography:
    """
    class to compute GPS coordinates of US FIPS (county codes)
    """
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        
        self.input_data_dir = config["PATH"]["HDFS_DOWNLOAD_DIR"]
        self.local_data_dir = config["PATH"]["LOCAL_DOWNLOAD_DIR"]
        self.output_path =  config["PATH"]["STAGE_DATA_DIR"]

        
    def read_data(self):
        logging.debug("Reading Data")
        
        nyt_covid = "us-counties.csv"
        self.covid_daily_perfips = self.spark.read.csv( os.path.join(self.input_data_dir, "COVID",nyt_covid), header = True)\
            .withColumn("date", F.to_date("date"))\
            .withColumn("cases", col("cases").cast(T.IntegerType()) )\
            .withColumn("deaths", col("deaths").cast(T.IntegerType()) )

        # load gazetteer (geographic coordinates for all county fips)
        fields = OrderedDict( [
            ( "state" , "string"),
            ("county" , "string"),
            ("fips" , "int"),
            ( "latitude" , "float"), 
            ("longitude" , "float") 
        ] )

        exprs = [ f"CAST(parsed['{field}'] AS {fld_type}) AS {field}" for field, fld_type in fields.items() ]

        self.gazetteer = self.spark.read.csv(os.path.join(self.input_data_dir, "2020_Gaz_counties_national.txt")).withColumn("parsed", ParseGazetteerUDF("_c0")).selectExpr( *exprs)

    def process_county_locations(self):
        """
        retrieve GPS coordinates of FIPS (county code) of covid data per county
        """
        
        logging.debug("Processing county locations")
        # check that FIPS are distinct, i.e. that there is only 1 FIPS per (state, county)
        nyt_locations = self.covid_daily_perfips.select("state", "county", "fips").distinct()
        fips_multi_locations = nyt_locations.groupby("fips").count().where("count > 1")
        print("Showing locations with identical FIPS")
        fips_multi_locations.show()
        print("=> multiple locations with null FIPS")
        # Match non null FIPS with gazetteer (geographic information for each US county)
        nyt_locations_with_fips = nyt_locations.where(col("fips").isNotNull())

        nyt_locations_with_geography = nyt_locations_with_fips.alias("a")\
            .join( self.gazetteer.alias("b"), ["fips"])\
            .select("a.*", "b.latitude", "b.longitude" )

        # locations that have not been found in gazetteer (null FIPS, or FIPS with unknown value)
        nyt_notmatched = nyt_locations.join(nyt_locations_with_geography, ["fips"], how = "left_anti")

        self.df_nyt_notmatched = nyt_notmatched.toPandas()
        
        self.process_unmatched_locations()
        
        # merge automatically matched with manually matched locations
        self.nyt_geography = nyt_locations_with_geography.union(self.fixed_gps)
        # remove remaining unmatched locations from dataset
        self.nyt_geography = self.nyt_geography.where( ( ~ F.isnan("latitude") ) | (~ F.isnan("longitude")) )
        
        # add primary key
        self.nyt_geography = self.nyt_geography.withColumn("location_id", F.monotonically_increasing_id())


    def process_unmatched_locations(self):
        """
        manually input GPS coordinates of unmatched locations 
        (state only, cities or territories without FIPS)
        """

        logging.debug("Processing unmatched locations")
        states_gps = pd.read_csv( os.path.join(self.local_data_dir, "US_states_GPS.csv"), sep = ",")
        states_gps.columns = states_gps.columns.str.lower()
        states_gps.loc[states_gps["state"] == "Washington State", "state"]= "Washington"

        gps_no_county = self.df_nyt_notmatched.loc[self.df_nyt_notmatched["county"] =="Unknown"].merge( states_gps, how = "left", on = "state")
        gps_no_county[ gps_no_county["latitude"].isna() | gps_no_county["longitude"].isna()]


        gps_cities = self.df_nyt_notmatched[ self.df_nyt_notmatched["county"] != "Unknown"].copy()


        # GPS of unincorporated territories
        gps_no_county.loc[ (gps_no_county["county"] == "Unknown") & (gps_no_county["state"]== "Puerto Rico") , "latitude"] = 18.2223
        gps_no_county.loc[ (gps_no_county["county"] == "Unknown") & (gps_no_county["state"]=="Puerto Rico"), "longitude"] = -66.4303
        gps_no_county.loc[ (gps_no_county["county"] == "Unknown") & (gps_no_county["state"]=="Virgin Islands"), "latitude"] = 18.34
        gps_no_county.loc[ (gps_no_county["county"] == "Unknown") & (gps_no_county["state"]=="Virgin Islands"), "longitude"] = -64.90
        gps_no_county.loc[ (gps_no_county["county"] == "Unknown") & (gps_no_county["state"]=="Guam"), "latitude"] = 13.4440
        gps_no_county.loc[ (gps_no_county["county"] == "Unknown") & (gps_no_county["state"]=="Guam"), "longitude"] = 144.7671
        # GPS coordinates of Saipan 15°11′N 145°45′E
        gps_no_county.loc[ (gps_no_county["county"] == "Unknown") & (gps_no_county["state"]=="Northern Mariana Islands"), "latitude"] = 15.16
        gps_no_county.loc[ (gps_no_county["county"] == "Unknown") & (gps_no_county["state"]=="Northern Mariana Islands"), "longitude"] = 145.7


        gps_cities["latitude"] = np.nan
        gps_cities["longitude"] = np.nan
        # Cities and metropolitan areas
        gps_cities.loc[ gps_cities["county"] == "New York City", "latitude"] = 40.712740
        gps_cities.loc[ gps_cities["county"] == "New York City", "longitude"] = -74.005974
        gps_cities.loc[ gps_cities["county"] == "Kansas City", "latitude"] = 39.099724
        gps_cities.loc[ gps_cities["county"] == "Kansas City", "longitude"] = -94.578331
        gps_cities.loc[ gps_cities["county"] == "Joplin", "latitude"] = 37.0842
        gps_cities.loc[ gps_cities["county"] == "Joplin", "longitude"] = -94.5133
        # FIPS unknown in Census Bureau gazetteer
        gps_cities.loc[ gps_cities["county"] == "St. Croix", "latitude"] =17.73
        gps_cities.loc[ gps_cities["county"] == "St. Croix", "longitude"] = -64.78
        # GPS for Cruz Bay (main city) 18.329936603847486, -64.79413842601294
        gps_cities.loc[ gps_cities["county"] == "St. John", "latitude"] = 18.33
        gps_cities.loc[ gps_cities["county"] == "St. John", "longitude"] = -64.794
        # GPS for Charlotte Amalie (main city) 18.341684050871354, -64.93175706594377
        gps_cities.loc[ gps_cities["county"] == "St. Thomas", "latitude"] = 18.34
        gps_cities.loc[ gps_cities["county"] == "St. Thomas", "longitude"] = -64.93
        gps_cities.loc[ gps_cities["county"] == "Valdez-Cordova Census Area", "latitude"] = 61.129050
        # GPS of Valdez
        gps_cities.loc[ gps_cities["county"] == "Valdez-Cordova Census Area", "longitude"] = -146.360130
        # Saipan 15.216501472234945, 145.72103373823464
        gps_cities.loc[ gps_cities["county"] == "Saipan", "latitude"] = 15.27
        gps_cities.loc[ gps_cities["county"] == "Saipan", "longitude"] = 145.72
        # Tinian 14.978910978711687, 145.63629283555494
        gps_cities.loc[ gps_cities["county"] == "Tinian", "latitude"] = 14.98
        gps_cities.loc[ gps_cities["county"] == "Tinian", "longitude"] = 145.636


        df_fixed_gps = pd.concat( [gps_no_county, gps_cities] )

        self.fixed_gps = self.spark.createDataFrame(df_fixed_gps)
    
    def write_data(self):
        """
        output locations table 
        """
        logging.debug("Writing location data")
        out_path = os.path.join(self.output_path, "nyt_locations_geography")
        self.nyt_geography.repartition(10)\
            .write\
            .option("header","true")\
            .mode("overwrite")\
            .csv(out_path)

    def ETL(self):
        """
        read raw locations data, transform it and output locations table
        """
        self.read_data()
        self.process_county_locations()
        self.write_data()


def main(configname = "capstone.cfg"):
    """
    main to test ETL for locations data
    """
    config = configparser.ConfigParser()
    config.read(configname)
    spark = create_spark_session()
    
    counties_geography = CountiesGeography(spark, config)       
    counties_geography.ETL()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", default = "capstone.cfg")
    l_args = parser.parse_args()
    
    main(l_args.config)
    
