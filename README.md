# DWH Engineer Challenge
In this challenge, deployable (docker container) ETL jobs are developed by using Python programming.
## Task Details:
- **Source System:** s3://gfg.challenge.dwh.data
- **Target System:** s3://gfg.challenge.dwh.sarkan
- **Source files:** Pipe (|) separated compressed (gzip) text files with 8 columns
- **Data cleaning steps:**
	- Date conversion: Two timestamp columns are parsed and converted to date strings during the file read operation (column 6 and column 7).
	- Duplication cleaning: Duplicated rows (according to columns 1,7,8) are removed. Latest value is kept.
- **Output files:** Date based splitted (column 7) parquet files with snappy compression.
- **Python Libraries:**
	- Boto3: It is used for AWS S3 operations (Read/Write/etc.).
	- Pandas: It is used for data manipulation operations with dataframes.
	- Logging: It is used for standart logging.
	- Configparser: It is used to manage config file and parameters.
- **Deployable Approach:** Docker container is created to implemenet deployable package. To create Docker container:
	- Clone the git project: git clone https://github.com/onursarkan/gfg
	- Update the aws credentials in common.conf file.
	- Build a docker image: sudo docker build -t gfgetl .
	- Run the docker image: docker run gfgetl
## Solution 1 (Delete-Insert ETL):
In this solution, DELETE/INSERT ETL approach is implemented.
- Step 1: Target folder is cleaned in target bucket (Parquet files are deleted).
	- Target s3 folder: s3://gfg.challenge.dwh.sarkan/solution1
- Step 2: All files are read from source s3 bucket as pandas dataframes.
	- During the read operations, date columns are cleaned (columns 6,7).
- Step 3: All dataframes are merged (concatenation).
- Step 4: Single merged data frame is cleaned from duplicated records (according to columns 1,7,8).
- Step 5: Merged dataframe is split to small dataframes according to date (column 7).
- Step 6: Small dataframes are written to target s3 bucket as snappy parquet files.
## Solution 2 (Incremental-Update ETL):
In this solution, INCREMENTAL-UPDATE ETL approach is implemented.
- Step 1: Processed file list is read from s3.
- Step 2: Source file list is prepared and compared with processed file list.
	- One by one, unprocessed files are read from source s3 bucket as pandas dataframes.
	- During the read operations, date columns are cleaned (columns 6,7).
- Step 3: Read data(pandas dataframe) is split to small dataframes according to date (column 7).
- Step 4: For each date based splitted data frame, related parquet file is read from s3.
	- Two data frames are concatenated and cleaned from duplications. New cleaned dataframe is saved into s3 as parquet file. If there is no previous parquet file, new file is created.
- Step 5: Processed file name is written to processed file list on s3. Next unprocessed file is processed.
## Logging Mechanism:
Python standart logging library (**logging**) is used to handle ETL logs. Log files are stored in s3 bucket (**s3://gfg.challenge.dwh.sarkan/logs/**).
## Lock Mechanism:
To prevent from concurrent ETL runs, lock file is created on s3 bucket (**s3://gfg.challenge.dwh.sarkan/etl_lock**) at the beginning of the ETL job. If another ETL job starts, it stops immediately because of lock file. Lock file is deleted when ETL job is completed or crushed.
## Configuration Management & Data Security:
Configuration parameters and sensitive data (passwords, secret keys,etc.) are stored in **common.conf** file. To work with configuration file, **configparser** library is used.
## Future work:
In this challenge task, there are two root causes for possible performance problems:
- In memory bulk data operations: After read operations, data is stored and manipulated in memory. If file sizes increase, memory lacks can be observed.
- Updating parquet files: To prevent from data duplication, parquet files are read from s3 bucket, and replaced with new versions. There is a high cost.
**ELT Approach with MPP architecture DB** can be good solution to solve these two performance problems.
