## Project Description

### Background

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Architecture

This project uses 2 AWS services - S3 and Redshift.

Data sources come from two public S3 buckets (user activity and songs metadata) in the form of JSON files.

The Redshift service is where data will be both ingested and transformed into a set of dimensional tables that facilitates analytics.

## Database schema justification

We use a Star schema here. It is a denormalized schema that provides the following benefits:
1) queries are simpler to express - less JOINs involved
2) queries are more performant 
3) optimized for business reporting queries 

#### Staging Tables
```
staging_events
staging_songs
```

#### Dimension Tables

```
users
songs
artists
time
```

#### Fact Tables
The `songplays` table is the core of this schema and contains foreign keys to four tables:
```
start_time REFERENCES time(start_time)
user_id REFERENCES time(start_time)
song_id REFERENCES songs(song_id)
artist_id REFERENCES artists(artist_id)
```
Each records has attributes referencing to the dimension tables.

## Project structure:
```
# config file
dhw.cfg

# contains all sql queries
sql_queries.py

# pre-run step of creating tables
create_tables.py

# run ETL
etl.py
```

## How to run:

You need to have an AWS Redshift Cluster up and running and an IAM role associated with it with the _correct_ permissions `AmazonS3ReadOnlyAccess, AmazonRedshiftFullAccess`. 
Create these 2 AWS resources and use the credentials to fill in the missing values in the template file `dwh.cfg`

Run the following scripts:
```
# set up the staging and analytical tables
python create_tables.py

# extract data from JSON files in S3, stage it in redshift, and store it in the dimensional tables.
python etl.py
```

