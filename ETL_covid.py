import argparse
import configparser

import location_geography
import preprocess_weather
import map_fips_GPS
import process_covid_counties

from create_spark_session import *


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", default = "capstone.cfg")
    l_args = parser.parse_args()
    
    # read config file
    config = configparser.ConfigParser()
    config.read(l_args.config)
    # create spark session
    spark = create_spark_session()
    
    # extract county locations table
    counties_geography = location_geography.CountiesGeography(spark, config)       
    counties_geography.ETL()

    # preprocess weather data to filter out irrelevant data
    preproc_weather = preprocess_weather.PreprocessWeather(spark, config)       
    preproc_weather.preprocess()
    
    # map weather stations to counties
    map_counties_stations = map_fips_GPS.MapCountiesToWeatherStations(spark, config)
    map_counties_stations.process( preproc_weather.weather_per_stations_per_measurement, 
                                  counties_geography.nyt_geography, 
                                  preproc_weather.weather)

    # process and outputs covid data
    covid_per_county = process_covid_counties.CovidPerCounty(spark, config)       
    covid_per_county.ETL(counties_geography.covid_daily_perfips, counties_geography.nyt_geography)
