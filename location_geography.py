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
import numpy as np

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


def load_parse_gazetteer(path, spark):
    '''
    load gazetteer files from Census Bureau (geographic coordinates for all county fips)
    county column can have an arbritary number of field, so need to provide additional parse mechanism
    '''
    fields = OrderedDict( [
        ( "state" , "string"),
        ("county" , "string"),
        ("fips" , "int"),
        ( "latitude" , "float"), 
        ("longitude" , "float") 
    ] )
    exprs = [ f"CAST(parsed['{field}'] AS {fld_type}) AS {field}" for field, fld_type in fields.items() ]

    gazetteer = spark.read.csv(path)\
    .withColumn("parsed", ParseGazetteerUDF("_c0")).selectExpr( *exprs)

    return gazetteer

def load_states(path):
    states_gps = pd.read_csv( path, sep = ",")
    states_gps.columns = states_gps.columns.str.lower()
    states_gps.loc[states_gps["state"] == "Washington State", "state"]= "Washington"
    return states_gps

def main():
    spark = create_spark_session()
    
    nyt_covid = "us-counties.csv"
    covid_daily_counties = spark.read.csv( os.path.join(project_path, "DATA", "COVID",nyt_covid), header = True)
    
    gazetteer = load_parse_gazetteer(os.path.join(project_path, "DATA", "2020_Gaz_counties_national.txt") , spark
    )
    
    #gazetteer.printSchema()
    all_gaz_locations = gazetteer.select("state", "county", "fips").distinct()
    # check that fips are unique in gazetteer, to perform join
    gaz_locations_per_fips = all_gaz_locations.groupby("fips").count().where("count > 1")
    print(f"locations in  gazetteer: {all_gaz_locations.count()}")
    print(f"locations in gazetteer with duplicated fips : {gaz_locations_per_fips.count()}")
   
    nyt_locations = covid_daily_counties.select("state", "county", "fips").distinct()
    fips_multi_locations = nyt_locations.groupby("fips").count().where("count > 1")
    # locations with same fips are when fips is null => remove null fips
    nyt_locations_with_fips = nyt_locations.where(col("fips").isNotNull())
    print(f"nyt locations : {nyt_locations.count()}")
    print(f"nyt locations with non-null fips: {nyt_locations_with_fips.count()}")
    # join with gazetteer to get geographic coordinates for counties fips
    nyt_locations_with_geography = nyt_locations_with_fips.alias("a").join( gazetteer.alias("b"), ["fips"])\
    .select("a.*", "b.latitude", "b.longitude" )
    # all locations that have not been matched during join (either because fips is null, or because fips is not found in gazetteer
    nyt_notmatched = nyt_locations.join(nyt_locations_with_geography, ["fips"], how = "left_anti")
    print(f"nyt locations that have been matched with gazetteer: {nyt_locations_with_geography.count()}")
    print(f"nyt locations that have not been matched with gazetteer: {nyt_notmatched.count()}")

    # manual processing of unmatched locations (done in pandas because there are only a few unmatched locations)
    df_nyt_notmatched = nyt_notmatched.toPandas()
    
    # step 1 : get GPS for states (county = Unknown)
    states_gps = load_states( os.path.join(project_path, "DATA", "US_states_GPS.csv"))    
    gps_no_county = df_nyt_notmatched.loc[df_nyt_notmatched["county"] =="Unknown"].merge( states_gps, how = "left", on = "state")
    print(f"locations with state only : {len(gps_no_county)}")

    # step 2 : manual set GPS of unincorporated territories
    gps_no_county.loc[ (gps_no_county["county"] == "Unknown") & (gps_no_county["state"]== "Puerto Rico") , "latitude"] = 18.2223
    gps_no_county.loc[ (gps_no_county["county"] == "Unknown") & (gps_no_county["state"]=="Puerto Rico"), "longitude"] = -66.4303
    gps_no_county.loc[ (gps_no_county["county"] == "Unknown") & (gps_no_county["state"]=="Virgin Islands"), "latitude"] = 18.34
    gps_no_county.loc[ (gps_no_county["county"] == "Unknown") & (gps_no_county["state"]=="Virgin Islands"), "longitude"] = -64.90
    gps_no_county.loc[ (gps_no_county["county"] == "Unknown") & (gps_no_county["state"]=="Guam"), "latitude"] = 13.4440
    gps_no_county.loc[ (gps_no_county["county"] == "Unknown") & (gps_no_county["state"]=="Guam"), "longitude"] = 144.7671
    gps_no_county.loc[ (gps_no_county["county"] == "Unknown") & (gps_no_county["state"]=="Northern Mariana Islands"), "latitude"] = 15.16
    gps_no_county.loc[ (gps_no_county["county"] == "Unknown") & (gps_no_county["state"]=="Northern Mariana Islands"), "longitude"] = 145.7

    # step 3 : manually set GPS for cities or metropolitan areas 
    gps_cities = df_nyt_notmatched[ df_nyt_notmatched["county"] != "Unknown"].copy()
    print(f"Cities and metropolitan areas : { len(gps_cities)}")
    gps_cities["latitude"] = np.nan
    gps_cities["longitude"] = np.nan
    # Cities and metropolitan areas
    gps_cities.loc[ gps_cities["county"] == "New York City", "latitude"] = 40.712740
    gps_cities.loc[ gps_cities["county"] == "New York City", "longitude"] = -74.005974
    gps_cities.loc[ gps_cities["county"] == "Kansas City", "latitude"] = 39.099724
    gps_cities.loc[ gps_cities["county"] == "Kansas City", "longitude"] = -94.578331
    gps_cities.loc[ gps_cities["county"] == "Joplin", "latitude"] = 37.0842
    gps_cities.loc[ gps_cities["county"] == "Joplin", "longitude"] = -94.5133
    # FIPS unknown in Census Bureau gazetteer (unincorporated territories)
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
    
    # step 4 : concatenate fixes for states with fixes for cities
    df_fixed_gps = pd.concat( [gps_no_county, gps_cities] )
    print(f"Manually fixed locations : { len(df_fixed_gps)}")
    
    # Finally, combine manual and automatic location matches
    fixed_gps = spark.createDataFrame(df_fixed_gps)
    nyt_geography = nyt_locations_with_geography.union(fixed_gps)
    print(f"Total number of locations : { nyt_geography.count()}")
    
    out_path = os.path.join(project_path, "OUT_DATA", "nyt_locations_geography")
    nyt_geography.write.partitionBy("state").format("parquet").save(out_path)

if __name__ == "__main__":
    main()
