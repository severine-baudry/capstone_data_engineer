#!/usr/bin/env python
# coding: utf-8

# In[170]:


import configparser
import os
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import MapType, StringType, DateType
from collections import OrderedDict
import pandas as pd
import numpy as np
import argparse

from create_spark_session import *

@udf(StringType())
def parse_race_ethnicity(line):
    return line.split(",")[0]


class ProcessCDCData():
    def __init__(self):
        self.spark = spark
        self.config = config
        
        self.input_data_dir = config["PATH"]["HDFS_DOWNLOAD_DIR"]
        self.output_path =  config["PATH"]["STAGE_DATA_DIR"]


    def read_data(self):
       '''
       read covid dataset (cases per population group)
       '''
        logging.debug("Reading Data")
 
        df_cdc_raw = self.spark.read.json(os.path.join( self.input_data_dir, "COVID","covid_by_pop_group.json"),
                        multiLine = True)
        self.df_cdc_raw = df_cdc_raw\
            .withColumn("race_ethnicity_combined", parse_race_ethnicity("race_ethnicity_combined"))\
            .withColumn("cdc_case_earliest_dt", col("cdc_case_earliest_dt").cast(DateType()))
    
    def create_age_table(self):
        ''' create age dimension table
        '''
        self.dim_age = self.spark.createDataFrame(
            data = [
                    (0,  '0 - 9 Years'),
                    (10, '10 - 19 Years'),
                    (20, '20 - 29 Years'),
                    (30, '30 - 39 Years'),
                    (40, '40 - 49 Years'),
                    (50, '50 - 59 Years'),
                    (60, '60 - 69 Years'),
                    (70, '70 - 79 Years'),
                    (80, '80+ Years'),
                    (1000,'Missing'),
                    (2000,'NA') ],
            schema = ["age_group_id", "age_group"]

        )
    def create_race_table(self):
 
        self.dim_race_ethnicity = self.spark.createDataFrame(
            data = [ (0, 'American Indian/Alaska Native', False),
                    (1, 'Asian', False),
                    (2, 'Black', False),
                    (3, 'Native Hawaiian/Other Pacific Islander', False),
                    (4, 'White', False),
                    (5, 'Hispanic/Latino', True),
                    (10, 'Multiple/Other', None),
                    (1000, 'Missing', None),
                    (2000, 'NA', None),
                    (3000, 'Unknown', None)
                ],
            schema = ["race_ethnicity_id", "race", "Hispanic_Latino"]
        )

    def create_sex_table(self):


        self.dim_sex = self.spark.createDataFrame(
            data = [ (0, 'Female'), 
                    (1, 'Male'), 
                    (2, 'Other'), 
                    (1000, 'Missing'), 
                    (2000, 'NA'), 
                    (3000, 'Unknown')        
                    ],
            schema = [ "sex_id", "sex"]
        )

    def create_date_table(self):
        self.df_dates = self.df_cdc_raw.select("cdc_case_earliest_dt").distinct().sort(col("cdc_case_earliest_dt").desc() )
        self.df_dates = df_dates.withColumn("date", col("cdc_case_earliest_dt").cast(DateType()))


    def create_fact_table(self):
         '''
         create the fact table by joining raw cdc table with dimension tables
         '''
        self.df_cdc = self.df_cdc_raw\
                .join( dim_age, on = "age_group", how = "left_outer")\
                .withColumnRenamed("race_ethnicity_combined", "race")\
                .join(dim_race_ethnicity, on = "race", how = "left_outer")\
                .join(dim_sex, on = "sex", how = "left_outer")\
                .select("cdc_case_earliest_dt","sex_id","age_group_id", "race_ethnicity_id", "count")


    def check_non_null(self):
        '''
        check that the fact table does not contain null entries
        '''

        # query : rows with null values (in any columns)
        cond_null = " OR ".join( [ f"'{a}' IS NULL" for a in self.df_cdc.columns])
        cdc_null = self.df_cdc.filter( cond_null)
        count = cdc_null.count()
        print(f"found {count} ")
    
    def write(self):
        '''
        write fact and dimension tables to parquet
        '''
        table_name = os.path.join(self.output_path, "covid_per_popgroup")
        self.df_cdc.write\
            .format("parquet")\
            .mode("overwrite")\
            .save(table_name )

        table_name = os.path.join(self.output_path, "dim_age_group")
        self.dim_age.write\
            .format("parquet")\
            .mode("overwrite")\
            .save(table_name )

        table_name = os.path.join(self.output_path, "dim_sex")
        self.dim_sex.write\
            .format("parquet")\
            .mode("overwrite")\
            .save(table_name )
        
        table_name = os.path.join(self.output_path, "dim_race_ethnicity")
        self.dim_race_ethnicity.write\
            .format("parquet")\
            .mode("overwrite")\
            .save(table_name )


# In[ ]:




