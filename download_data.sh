#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage : $0 download_dir"
    exit
fi



wd=$PWD
echo $wd

daily=https://www1.ncdc.noaa.gov/pub/data/ghcn/daily
ldaily=(${daily}/ghcnd-stations.txt ${daily}/ghcnd-states.txt ${daily}/ghcnd-countries.txt ${daily}/readme.txt)
lweather=( ${ldaily[@]} ${daily}/by_year/2020.csv.gz ${daily}/by_year/2021.csv.gz)

echo $1
mkdir $1
cd $1
mkdir WEATHER
mkdir COVID
failed=0

# attempts to download data 10 times, because some sites limit the number of files which can be donwloaded per minute
for i in {1..10}; do

    # weather files
    outdir=WEATHER
    
    for url in ${lweather[@]}; do
        echo $url
        base_url=${url##*/}
        echo $base_url
        if [ ! -f ${outdir}/${base_url} ]; then
            echo $base_url does not exist
            wget $url -P ${outdir}
            if [ $? -ne 0 ]; then
                echo getting $url failed
                failed=1
            fi
        fi
    done
    
    # covid per county data
    outdir=COVID
    url=https://github.com/nytimes/covid-19-data/raw/master/us-counties.csv
    base_url=${url##*/}
    echo $base_url
    if [ ! -f ${outdir}/${base_url} ]; then
        echo $base_url does not exist
            wget $url -P ${outdir}
        if [ $? -ne 0 ]; then
            echo getting $url failed
            failed=1
        fi
    fi
    
    # gazetteer file
    url=https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2020_Gazetteer/2020_Gaz_counties_national.zip
    base_url=${url##*/}
    echo $base_url
    if [ ! -f ${base_url} ]; then
        echo $base_url does not exist
        wget $url
        if [ $? -ne 0 ]; then
            echo getting $url failed
            failed=1
        else
            unzip 2020_Gaz_counties_national.zip
        fi
    fi
    
    
    # check if all downloads succeeded
    if [ ${failed} -eq 0 ]; then
        break
    fi
    sleep 10
done

cd $wd

