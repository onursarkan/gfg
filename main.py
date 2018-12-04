# Main ETL Job
import util as u

u.drop_etl_lock()

u.log.warning('\n'\
'    Main ETL Job is started...\n'\
'    In this task two different ETL approaches are implemented: \n'\
'    Solution 1: DELETE-INSERT ETL Approach \n'\
'    Solution 2: INCREMENTAL-UPDATE ETL Approach \n'\
'    Log File: s://'+u.target_bucket+'/'+u.log_file_name+'\n')


u.log.warning('\n'\
'    Solution 1 (DELETE-INSERT ETL) is started...\n' \
'    Source s3 Bucket: '+u.src_bucket+' \n' \
'    Target s3 Destination: '+u.target_bucket+'\solution1 \n' \
'    Output files: Date based splitted Parquet files with snappy compression \n')
u.solution1()

u.log.warning('\n'\
'    Solution 2 (INCREMENTAL-UPDATE) is started...\n' \
'    Source s3 Bucket: '+u.src_bucket+' \n' \
'    Target s3 Destination: '+u.target_bucket+'\solution2 \n' \
'    Output files: Date based splitted Parquet files with snappy compression \n')
u.solution2()
