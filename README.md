# Project: Data Lake

## Summary
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Data
Data used in this project is available in udacity open s3 bucket
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

Sample data is available in the `data/` folder.

## Schema

Star schema optimized for queries on song play analysis. This includes the
following tables.

### Fact Table
songplays

### Dimension Tables
users
songs
artists
time

### Configuration

Set up a config file `dl.cfg` that uses the following schema. Put
in the information for your IAM-Role that can read and write S3 buckets.
```cfg
[aws]
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```
### ETL pipeline

Simply run the ETL script.

```bash
python etl.py
```
