import configparser
import os
import urllib
import requests
from urllib.error import *
from urllib.parse import urljoin
import zipfile
import argparse

config = configparser.ConfigParser()
config.read("capstone.cfg")

local_data_dir = config["PATH"]["LOCAL_DATA_DIR"]


def download_weather_data(out_dir):
    # TODO : create directories if needed
    url_dir = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/"
    weather_2020 = "2020.csv.gz"
    data_dir = os.path.join(url_dir,"by_year")
    #print(data_dir)
    url_weather = os.path.join( data_dir , weather_2020)
    #print(url_weather)
    out_weather = os.path.join(out_dir, weather_2020)
    try :
        urllib.request.urlretrieve(url_weather, out_weather)
    except  Exception as inst:
        print( f"unable to download {url_weather} to {out_weather}" )
        print(inst)
        return

    for weather_file in ["ghcnd-stations.txt", "ghcnd-states.txt", "ghcnd-countries.txt", "readme.txt"]:
        url = urljoin(url_dir, weather_file)
        out = os.path.join(out_dir,  weather_file)
        try :
            urllib.request.urlretrieve(url, out)
        except :
            print( f"unable to download {url} to {out}" )
        
def download_covid_percounty(out_dir):
    nyt_dir = "https://github.com/nytimes/covid-19-data/raw/master/"
    nyt_covid  = "us-counties.csv"
    url = urljoin(nyt_dir, nyt_covid)
    out = os.path.join(out_dir, nyt_covid) 
    urllib.request.urlretrieve(url, out)
    
def download_gazetteer(out_dir):
    gazetteer_dir = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2020_Gazetteer/"
    gazetteer_name ="2020_Gaz_counties_national.zip"
    url = urljoin(gazetteer_dir, gazetteer_name)
    out = os.path.join(out_dir, gazetteer_name)
    urllib.request.urlretrieve(url, out)
    with zipfile.ZipFile(out, 'r') as zip:
        if len( zip.infolist()) != 1 :
            print(f"error :zip does not contain asingle file {len( zip.infolist())}")
            raise ValueError
        uncompress = zip.infolist()[0].filename
        zip.extractall()
        outname = os.path.join(out_dir, uncompress)
        #print(outname, uncompress)
        with open(outname, "w") as fs :
            with open(uncompress) as f :
                i=0
                for line in f :
                    if i != 0:                 
                        fs.write(line)
                    i += 1    
        os.remove(uncompress)
        
def main():
    out_dir = "DATA"
    weather_dir = os.path.join(out_dir, "WEATHER")
    covid_dir = os.path.join( out_dir, "COVID")
    
    try :
        os.makedirs(out_dir)
    except FileExistsError :
        pass
    try :
        os.makedirs(weather_dir)
    except FileExistsError :
        pass
    try :
        os.makedirs(covid_dir)
    except FileExistsError :
        pass
   
    download_weather_data(weather_dir)
    download_covid_percounty(covid_dir)
    download_gazetteer(out_dir)
            
if __name__ == "__main__":
    main()
