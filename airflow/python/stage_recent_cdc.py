import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import MapType, StringType
from operator import add

import sys
from random import random
import logging
import os
from collections import OrderedDict
import argparse
from sodapy import Socrata

log_marker= "BLABLABLA-- "

def my_log(logger, msg):
    logger.warning( log_marker + msg)


if __name__ == "__main__":
    #log = logging.getLogger("py4j")  
    log = logging.getLogger("pyspark")  
    log.setLevel(logging.NOTSET)
    my_log(log,f"ARGS :  {sys.argv} ")
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default = "data.cdc.gov")
    parser.add_argument("--dataset_identifier", default = "vbim-akqf")
    parser.add_argument("--timeout", default = 100)
    parser.add_argument("--table", default = "recent_cdc")
    parser.add_argument("--apptoken", required = True)
    parser.add_argument("--last_date", required = True)
    
    l_args = parser.parse_args()
    print(l_args)
    
    spark = SparkSession\
        .builder\
        .appName("stage_recent_cdc")\
        .getOrCreate()
    

    # connect to cdc 
    client = Socrata(l_args.url,
                l_args.apptoken,
                timeout = l_args.timeout)
    
    my_log(log, f"socrata app token = {l_args.apptoken}")
    #str_min_date = datetime(2021, 4,1).isoformat() + ".000"
    my_log(log, f"min date : {l_args.last_date}")
    # retrieve new covid cases per population group
    cases_per_date = client.get(l_args.dataset_identifier,
                        group = "cdc_case_earliest_dt, sex, age_group, race_ethnicity_combined",
                        select = "cdc_case_earliest_dt, sex, age_group, race_ethnicity_combined, count(*)"
                        ,where = f"cdc_case_earliest_dt > '{l_args.last_date}'",
                        limit = 200000,
                        content_type = "json"
                        )
    my_log(log, f"nb new records : {len(cases_per_date)}")
    # transform to data frame
    df_cases = spark.read.json(spark.sparkContext.parallelize(cases_per_date))
    my_log(log, f"nb rows in dataframe : {df_cases.count()}")
    # write to postgres
    
    df_cases.write\
            .format("jdbc")\
            .option("url", "jdbc:postgresql:capstone")\
            .option("dbtable", l_args.table)\
            .option("user","postgres")\
            .option("password", "postgres")\
            .mode("overwrite")\
            .save()
