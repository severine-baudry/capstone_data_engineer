#!/usr/bin/env python
# coding: utf-8

# Load covid cases per population group from Center of Diseases Control (https://data.cdc.gov/Case-Surveillance/COVID-19-Case-Surveillance-Public-Use-Data/vbim-akqf)
# The data list all individual cases (deidentified), with information on sex, age group, race and identity, and date of case, plus some additional information (hospitalisation, intensive care, death ...).
# As the dataset is huge (20.6 Mrows at mid-march), and since their are many identical rows (because of deidentification), we query the data through the api, grouped by the categories we're interested in. Thus, the downloaded dataset remains relatively small.

# Without an application token, the size of the downloaded data is limited (see https://dev.socrata.com/docs/app-tokens.html). 
# 
# CDC (Center of Diseases Control) uses the [Socrata Open Data API](https://dev.socrata.com/) to manage access to its data. To increase download capabilities, you have to create an AppToken.
# 
# First, [create a Socrata account](https://data.cdc.gov/signup). 
# 
# Then, [sign in](https://data.cdc.gov/login) to Socrata, using the Socrata ID field. Go to 'My Profile', then 'Edit profile', then tab 'Developer settings' (https://data.cdc.gov/profile/edit/developer_settings). Create an AppToken by following [this guide](https://support.socrata.com/hc/en-us/articles/210138558-Generating-an-App-Token).
# 
# Then store the AppToken into the config file `capstone.cfg` :
# ```
# [CDC]
# APPTOKEN=<MyAppToken>
# ```
# 

# documentation about Socrata API is available [here](https://dev.socrata.com/docs/queries/). Some exemple uses of the python client library  [sodapy](https://github.com/xmunoz/sodapy) can be found [here](https://github.com/xmunoz/sodapy/blob/master/examples/soql_queries.ipynb)

import pandas as pd
from sodapy import Socrata
import configparser
import json
import os
from datetime import datetime

import argparse
from create_spark_session import *



class DownloadCDCData():
    '''
    Download covid dataset from CDC (Center Of Disease Control), using the Socrate API
    The dataset contains the number of cases per date, sex, age group, race, and death status
    '''
    
    # identifier of CDC dataset with covid case surveillance (https://data.cdc.gov/Case-Surveillance/COVID-19-Case-Surveillance-Public-Use-Data/vbim-akqf)
    cdc_dataset_identifier = "vbim-akqf"
    def __init__(self, config_name="capstone.cfg"):
        self.config = configparser.ConfigParser()
        self.config.read(config_name)
        
        
    def connect(self, timeout = 100):
        '''
        Connect to CDC using AppToken for Socrata API
        increase default timeout to 100, since the query of such large dataset may take longer time tat default timeout
        '''
        app_token = self.config["CDC"]["APPTOKEN"]
        print(f"connecting to CDC server")
        self.client = Socrata("data.cdc.gov",
                        app_token,
                        timeout = timeout)
    
    def print_dataset_schema(self):
        '''
        show schema (column names) of the dataset
        '''
        metadata = self.client.get_metadata(DownloadCDCData.cdc_dataset_identifier)
        for x in metadata['columns']:
            print(x['name'] )

    def get_covid_per_popgroup(self,nbrows = 1000000):
        '''
        download nb of covid cases per date, sex, age group, race, death status
        '''
        # by default, max number of records returned is 1000. We have to increase it to get the full results
        print("Downloading CDC dataset")
        self.res_sex_age_race_date = self.client.get(DownloadCDCData.cdc_dataset_identifier, 
                     group = "cdc_case_earliest_dt, sex, age_group, race_ethnicity_combined, death_yn",
                     select = "cdc_case_earliest_dt, sex, age_group, race_ethnicity_combined, death_yn, count(*)",
                     limit = nbrows)
    def data_info(self):
        '''
        print some information about the downloaded dataset
        '''
        # number of different groups, and corresponding total number of rows (must match the total number of rows of the whole file)
        nb_rows = len(self.res_sex_age_race_date)
        total_cases = sum( [ int(row["count"]) for row in self.res_sex_age_race_date])
        print( f"nb rows : {nb_rows}")
        print( f"total cases : {total_cases}")
    
    def write(self):
        '''
        write data to json
        '''
        print("Writing data")
        with open(os.path.join(self.config["PATH"]["LOCAL_DOWNLOAD_DIR"], "COVID", "covid_by_pop_group.json"), "w") as fs :
            # Redshift requires flattened structure...
            for elem in self.res_sex_age_race_date :
                json.dump(elem, fs, indent = 2)
                fs.write("\n")

    def execute(self):
        self.connect()
        self.get_covid_per_popgroup()
        self.write()
        
def main(configname = "capstone.cfg"):
    """
    main to test ETL for locations data
    """
    download_cdc = DownloadCDCData(configname)
    download_cdc.execute()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", default = "capstone.cfg")
    l_args = parser.parse_args()
    
    main(l_args.config)
    


