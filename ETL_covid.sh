
# read download dir and hdfs dir from config file
configfile=capstone.cfg

a=`grep LOCAL_DOWNLOAD_DIR $configfile`
eval $a
a=`grep HDFS_DOWNLOAD_DIR $configfile`
eval $a
a=`grep STAGE_DATA_DIR $configfile`
eval $a
a=`grep S3_OUTPUT_PATH $configfile`
eval $a


echo "Local download dir is $LOCAL_DOWNLOAD_DIR"
echo "HDFS data dir is $HDFS_DOWNLOAD_DIR"
echo "Stage data dir is $STAGE_DATA_DIR"

#download data locally
echo "Downloading data to $LOCAL_DOWNLOAD_DIR"
./download_data.sh $LOCAL_DOWNLOAD_DIR 
mv US_states_GPS.csv $LOCAL_DOWNLOAD_DIR/
python api_cdc_data.py
hdfs dfs -put ${LOCAL_DOWNLOAD_DIR}/COVID/covid_by_pop_group.json ${STAGE_DATA_DIR}/

# copy data to HDFS for parallel processing with spark
echo "Moving data to HDFS $HDFS_DOWNLOAD_DIR"
hdfs dfs -mkdir ${HDFS_DOWNLOAD_DIR}
hdfs dfs -put ${LOCAL_DOWNLOAD_DIR}/* ${HDFS_DOWNLOAD_DIR}
# data processing pipeline
echo "Processing data"
python ETL_covid.py --config $configfile

echo "Creating redshift cluster"
python create_redshift_cluster.py

echo "Loading data to S3"
# s3-dist-cp is much faster than individual copy
s3-dist-cp --src=${STAGE_DATA_DIR} --dest=${S3_OUTPUT_PATH}

echo "Loading data from S3 to redshift"
python process_cdc_redshift.py 
python load_csv_to_redshift.py


