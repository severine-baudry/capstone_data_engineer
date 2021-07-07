
# read download dir and hdfs dir from config file
configfile=capstone.cfg

a=`grep LOCAL_DOWNLOAD_DIR $configfile`
eval $a
a=`grep HDFS_DOWNLOAD_DIR $configfile`
eval $a
a=`grep STAGE_DATA_DIR $configfile`
eval $a


echo "Local download dir is $LOCAL_DOWNLOAD_DIR"
echo "HDFS data dir is $HDFS_DOWNLOAD_DIR"
echo "Stage data dir is $STAGE_DATA_DIR"

#download data locally
echo "Downloading data to $LOCAL_DOWNLOAD_DIR"
./download_data.sh $LOCAL_DOWNLOAD_DIR 
mv US_states_GPS.csv $LOCAL_DOWNLOAD_DIR/
python api_cdc_data.py

# copy data to HDFS for parallel processing with spark
echo "Moving data to HDFS $HDFS_DOWNLOADx_DIR"
hdfs dfs -mkdir ${HDFS_DOWNLOAD_DIR}
hdfs dfs -put ${LOCAL_DOWNLOAD_DIR}/* ${HDFS_DOWNLOAD_DIR}

# data processing pipeline
echo "Processing data"
python ETL_covid.py --config $configfile

echo "Creating redshift cluster"

python create_redshift_cluster.py

echo "Loading data to redshift"
python parquet_to_redshift.py

