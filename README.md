# Project: Data Lake
# Author: Wamutu Eddie

## Intro:
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.As their data engineer, I have been tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.I'll be able to test my database and ETL pipeline by running queries given to me by the analytics team from Sparkify and compare my results with their expected results.
## Description:
In this project, I'll apply what I've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, I will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. I'll deploy this Spark process on a cluster using AWS.
## Thought process:
I decided to use a star schema model and also that data retrieval, processing and loading will take place in the cloud.I figured I'd use Amazon web services (AWS). Working in the cloud cuts down on latency issues since the two S3 buckets I'll be using are in different regions.

I took advantage of Apache Spark on top of a cluster powered by Amazon Elastic MapReduce, a web service that enables business firms, data analysts, data scientists, and developers to conveniently and cost-effectively process vast amounts of data. I have written a python script to transfer all the data from JSON files also stored in the cloud (Amazon S3) to the cluster. You can think of this python script as an automated pipeline. Then, after the data is processed and saved into parquet files, it is loaded to a repository in S3.

## Configurations
```dl.cfg``` is not provided here for security reasons. It contains :
```
KEY=YOUR_AWS_ACCESS_KEY
SECRET=YOUR_AWS_SECRET_KEY
```
Incase you are in the command line - Connecting to EMR cluster

 ```scp -i <.pem-file> <Local-Path> <username>@<EMR-MasterNode-Endpoint>:~<EMR-path>```
 
Submitting python script using ```spark-submit``` (Before running job make sure EMR Role have access to s3)

```spark-submit etl.py --master yarn --deploy-mode client --driver-memory 4g --num-executors 2 --executor-memory 2g --executor-core 2```

