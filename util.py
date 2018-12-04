# This python class contains main parameters and functions of ETL jobs
# Main parameters are read from common.conf configuration file.

import configparser
import boto3
import botocore
import pandas as pd
import io
import atexit
import logging
import datetime
import sys



###########################################################
# MAIN PARAMETERS:
# Configuration parser is initialized:
cp = configparser.RawConfigParser()
cp.read("./common.conf")

# Source and target s3 buckets:
src_bucket = cp.get('aws_params', 's3_source_bucket')
target_bucket = cp.get('aws_params', 's3_target_bucket')
# AWS credentials and region name:
aws_key = cp.get('aws_params', 'aws_key')
aws_secret_key = cp.get('aws_params', 'aws_secret_key')
aws_region = cp.get('aws_params', 'aws_region')
###########################################################
# BOTO3 S3 CLIENT AND RESOURCE MANAGER (To read and write files on S3):
s3_client = boto3.client('s3',aws_access_key_id=aws_key,aws_secret_access_key=aws_secret_key,region_name=aws_region)
s3_resource = boto3.resource('s3', aws_access_key_id=aws_key,aws_secret_access_key=aws_secret_key,region_name=aws_region)
###########################################################
# COMMON OBJECTS:
# Column Names:
column_names=['c1','c2','c3','c4','c5','c6','c7','c8']
###########################################################
# STANDART LOGGING ON S3 BUCKET:
def write_logs(body, bucket, key):
    s3_client.put_object(Body=body, Bucket=bucket, Key=key)
log_file_name='logs/'+datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")+'.log'
log = logging.getLogger('GFG_ETL')
log_stringio = io.StringIO()
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)
atexit.register(write_logs, body=log_stringio.getvalue(), bucket=target_bucket, key=log_file_name)
###########################################################
# ETL FUNCTIONS:
# save_to_s3: This function writes given dataframe to s3 bucket as parquet file with snappy compression.
def save_to_s3(df,keyName):
    df.to_parquet('tmp_file.snappy.parquet', compression='snappy')
    with open('tmp_file.snappy.parquet','rb') as f:
        s3_client.put_object(Body=f,Bucket=target_bucket,Key=keyName)
# dateparse: This function parses the datetime string and returns date string (YYYY-MM-DD).
#            This function is used during the file read operations with Pandas library.
def dateparse(x):
    try:
        return pd.to_datetime(x,format="%Y-%m-%d")
    except:
        return 'NaN'
# add_etl_lock: To prevent from duplicated ETL runs, lock file is added to s3 bucket.
def add_etl_lock():
    with open('etl_lock', 'rb') as lockfile:
        #lockfile.write(datetime.datetime.now().strftime("%Y-%m-%d_%H:%M"))
        s3_client.put_object(Body=lockfile, Bucket=target_bucket, Key='etl_lock')
# drop_etl_lock: Lock file is deleted from s3 after successful ETL job.
def drop_etl_lock():
    bucket = s3_resource.Bucket(target_bucket)
    bucket.objects.filter(Prefix='etl_lock').delete()
# is_etl_locked: This function returns boolean according to existence of lock file.
def is_etl_locked():
    bucket = s3_resource.Bucket(target_bucket)
    key = 'etl_lock'
    objs = list(bucket.objects.filter(Prefix=key))
    if len(objs) > 0 and objs[0].key == key:
        return True
    else:
        return False

# get_processed_list: This function returns processed file list for INCREMENTAL-UPDATE ETL.
def get_processed_list():
    try:
        obj = s3_client.get_object(Bucket=target_bucket, Key='processed_file_list')
        df = pd.read_csv(io.BytesIO(obj['Body'].read()),names=['file_name'])
        return df
    except:
        df = pd.DataFrame(columns=['file_name'])
        return df

# add_to_processed_list: This function adds new file name to processed file list for INCREMENTAL-UPDATE ETL.
def add_to_processed_list(df,file_name):
    df_new = df.append({'file_name': file_name},ignore_index=True)
    df_new.to_csv('processed_file_list', header=None,index=None, sep=' ', mode='w')
    keyName='processed_file_list'
    with open('processed_file_list', 'rb') as f:
        s3_client.put_object(Body=f, Bucket=target_bucket, Key=keyName)

# clean_processed_list: This function cleans the processed file list for fresh ETL job:
def clean_processed_list():
    bucket = s3_resource.Bucket(target_bucket)
    bucket.objects.filter(Prefix='processed_file_list').delete()

# Solution 1: In this job, DELETE/INSERT ETL approach is implemented.
#     Step 1: Target folder is cleaned in target bucket (Parquet files are deleted).
#             Target s3 folder: s3://gfg.challenge.dwh.sarkan/solution1
#     Step 2: All files are read from source s3 bucket as pandas dataframes.
#             During the read operations, date columns are cleaned (columns 6,7).
#     Step 3: All dataframes are merged (concatenation).
#     Step 4: Single merged data frame is cleaned from duplicated records (according to columns 1,7,8).
#     Step 5: Merged dataframe is split to small dataframes according to date (column 7).
#     Step 6: Small dataframes are written to target s3 bucket as snappy parquet files.
def solution1():
    log.warning('Solution 1: ETL Job is started.')
    # To prevent from duplicated ETL run, lock file is checked:
    if is_etl_locked():
        log.warning('Solution 1: There is a lock file. ETL Job is stopped.')
        return False
    else:
        add_etl_lock() # ETL lock is created to disable other runs.
        log.warning('Solution 1: ETL Lock file is created.')

    try:
        # STEP 1:
        bucket = s3_resource.Bucket(target_bucket)
        bucket.objects.filter(Prefix='solution1').delete()
        log.warning('Solution 1: Parquet files are deleted from target s3 bucket.')
        # STEP 2:
        # Empty dataframe is created with column list:
        df_main = pd.DataFrame(columns=column_names)
        # Source file names are determined for source s3 bucket:
        for key in s3_client.list_objects(Bucket=src_bucket)['Contents']:
            file_name = key['Key']
            # Source files are read as Pandas dataframes:
            obj = s3_client.get_object(Bucket=src_bucket, Key=file_name)
            df_new = pd.read_csv(io.BytesIO(obj['Body'].read()),compression='gzip',sep='|',index_col=False,
                             header=None,parse_dates=['c6','c7'],date_parser=dateparse,names=column_names)
        # STEP 3:
            frames = [df_main, df_new]
            df_main = pd.concat(frames)
        # STEP 4:
        df_clean=df_main.drop_duplicates(subset=['c1', 'c7', 'c8'], keep='last', inplace=False)
        log.warning('Solution 1: All files are read, merged and cleaned.')

        # STEP 5:
        for c7, df_day in df_clean.groupby('c7'):
        # STEP 6:
            # Date strings are parsed to create parquet file names:
            f_str=c7.strftime('%Y-%m-%d')
            y_str=c7.strftime('%Y')
            m_str=c7.strftime('%m')
            d_str=c7.strftime('%d')
            # Target parquet file name is produced according to date of records:
            key_name='solution1/year='+y_str+'/month='+m_str+'/day='+d_str+'/'+f_str+'.snappy.parquet'
            # Parquet file is created for specific date:
            save_to_s3(df_day, key_name)
        log.warning('Solution 1: Production of target parquet files is completed.')
        log.warning('Solution 1: ETL job is completed.')
        drop_etl_lock() # Lock file is cleaned after successful run.
        log.warning('Solution 1: ETL Lock file is cleaned.')
        return True
    except:
        log.error('Solution 1: ETL job is failed.')
        drop_etl_lock()  # Lock file is cleaned after ETL crush
        log.warning('Solution 1: ETL Lock file is cleaned.')
        return False


# Solution 2: In this job, INCREMENTAL-UPDATE ETL approach is implemented.
#     Step 1: Processed file list is read from s3.
#     Step 2: Source file list is prepared and compared with processed file list.
#             One by one, unprocessed files are read from source s3 bucket as pandas dataframes.
#             During the read operations, date columns are cleaned (columns 6,7).
#     Step 3: Read data(pandas dataframe) is split to small dataframes according to date (column 7).
#     Step 4: For each date based splitted data frame, related parquet file is read from s3.
#             Two data frames are concatenated and cleaned from duplications. New cleaned dataframe
#             is saved into s3 as parquet file. If there is no previous parquet file, new file is created.
#     Step 5: Processed file name is written to processed file list on s3. Next unprocessed file is processed.
def solution2():
    log.warning('Solution 2: ETL Job is started.')
    # To prevent from duplicated ETL run, lock file is checked:
    if is_etl_locked():
        log.warning('Solution 2: There is a lock file. ETL Job is stopped.')
        return False
    else:
        add_etl_lock() # ETL lock is created to disable other runs.
        log.warning('Solution 2: ETL Lock file is created.')

    try:
        # STEP 1:
        processed_files = get_processed_list()
        log.warning('Solution 2: Processed file list is read from s3')
        # STEP 2:
        # Source file names are determined for source s3 bucket:
        for key in s3_client.list_objects(Bucket=src_bucket)['Contents']:
            file_name = key['Key']
            # If source file is not already processed, it is read as Pandas dataframe:
            if file_name not in processed_files['file_name'].tolist():
                log.warning('Solution 2: File process started for:'+file_name)
                obj = s3_client.get_object(Bucket=src_bucket, Key=file_name)
                df = pd.read_csv(io.BytesIO(obj['Body'].read()),compression='gzip',sep='|',index_col=False,
                             header=None,parse_dates=['c6','c7'],date_parser=dateparse,names=column_names)
                # Duplicated raws are cleaned:
                df_clean = df.drop_duplicates(subset=['c1', 'c7', 'c8'], keep='last', inplace=False)
                # STEP 3:
                for c7, df_new in df_clean.groupby('c7'):
                    # STEP 4:
                    # Date strings are parsed to create parquet file names:
                    f_str=c7.strftime('%Y-%m-%d')
                    y_str=c7.strftime('%Y')
                    m_str=c7.strftime('%m')
                    d_str=c7.strftime('%d')
                    # Target parquet file name is produced according to date of records:
                    key_name='solution2/year='+y_str+'/month='+m_str+'/day='+d_str+'/'+f_str+'.snappy.parquet'
                    # Read the parquet file
                    buffer = io.BytesIO()
                    object = s3_resource.Object(target_bucket, key_name)
                    try:
                        object.download_fileobj(buffer)
                        df_old = pd.read_parquet(buffer)
                        frames = [df_old, df_new]
                        df_merged = pd.concat(frames)
                        df_merged = df_merged.drop_duplicates(subset=['c1', 'c7', 'c8'], keep='last', inplace=False)
                        save_to_s3(df_merged, key_name)
                    except botocore.exceptions.ClientError as e:
                        if e.response['Error']['Code'] == "404":
                            save_to_s3(df_new, key_name)
                        else:
                            raise
                log.warning('Solution 2: File process finished for:' + file_name)
                add_to_processed_list(processed_files,file_name)

        log.warning('Solution 2: Production of target parquet files is completed.')
        log.warning('Solution 2: ETL job is completed.')
        drop_etl_lock()  # Lock file is cleaned after successful run.
        log.warning('Solution 2: ETL Lock file is cleaned.')
        return True
    except:
        log.error('Solution 2: ETL job is failed.')
        drop_etl_lock()  # Lock file is cleaned after ETL crush
        log.warning('Solution 2: ETL Lock file is cleaned.')
        return False

