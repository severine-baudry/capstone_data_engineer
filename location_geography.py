import configparser
import os
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import udf, col
from pyspark.sql.types import MapType, StringType
from collections import OrderedDict
import pandas as pd

config = configparser.ConfigParser()
config.read("capstone.cfg")

os.chdir(config["PATH"]["project"])
project_path = config["PATH"]["project"]


def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("covid_DB") \
        .getOrCreate()
    
    return spark

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


def main():
    spark = create_spark_session()
    
    nyt_covid = "us-counties.csv"
    covid_daily_perfips = spark.read.csv( os.path.join(project_path, "DATA", "COVID",nyt_covid), header = True)
    #covid_daily_perfips.printSchema()
    
    # load gazetteer (geographic coordinates for all county fips)
    fields = OrderedDict( [
        ( "state" , "string"),
        ("county" , "string"),
        ("fips" , "int"),
        ( "latitude" , "float"), 
        ("longitude" , "float") 
    ] )

    exprs = [ f"CAST(parsed['{field}'] AS {fld_type}) AS {field}" for field, fld_type in fields.items() ]

    gazetteer = spark.read.csv(os.path.join(project_path, "DATA", "2020_Gaz_counties_national.txt"))\
    .withColumn("parsed", ParseGazetteerUDF("_c0")).selectExpr( *exprs)

    #gazetteer.printSchema()
    all_locations = gazetteer.select("state", "county", "fips").distinct()
    locations_per_fips = all_locations.groupby("fips").count().where("count > 1")
    locations_per_fips.show(10)
    
    

if __name__ == "__main__":
    main()
