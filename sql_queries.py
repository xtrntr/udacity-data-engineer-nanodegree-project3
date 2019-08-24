import configparser


# Load redshift and s3 configurations
config = configparser.ConfigParser()
config.read('dwh.cfg')

# SQL queries for dropping tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# SQL queries for creating tables
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession int,
    lastName varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration float,
    sessionId int,
    song varchar,
    status int,
    ts varchar,
    userAgent varchar,
    userId int
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs int,
    artist_id varchar,
    artist_latitude varchar,
    artist_longitude varchar,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year int
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
    songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
    start_time varchar NOT NULL,
    user_id varchar NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id varchar,
    location varchar,
    user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song (
    song_id varchar PRIMARY KEY,
    title varchar,
    artist_id varchar,
    year varchar,
    duration float
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist (
    artist_id varchar PRIMARY KEY,
    name varchar,
    location varchar,
    latitude varchar,
    longitude varchar
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour varchar,
    day varchar,
    week varchar,
    month varchar,
    year varchar,
    weekday varchar
)
""")

# SQL queries for copying data from s3 into redshift staging table
staging_events_copy = ("""
COPY staging_events from 's3://udacity-dend/log_data'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON 's3://udacity-dend/log_json_path.json';
""").format(config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/song_data'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto';
""").format(config['IAM_ROLE']['ARN'])

# SQL queries for inserting data into final tables in redshift
songplay_table_insert = ("""
INSERT INTO songplay (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT e.ts as start_time,
    e.userId as user_id,
    e.level as level,
    s.song_id as song_id,
    s.artist_id as artist_id,
    e.sessionId as session_id,
    e.location as location,
    e.userAgent as user_agent
FROM staging_events e
JOIN staging_songs s ON (e.song = s.title AND e.artist = s.artist_name)
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT userId as user_id,
    firstName as first_name,
    lastName as last_name,
    gender as gender,
    level as level
FROM staging_events
WHERE user_id IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO song (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT song_id as song_id,
    title as title,
    artist_id as artist_id,
    year as year,
    duration as duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artist (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT DISTINCT artist_id as artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second' as start_time,
    extract(HOUR FROM timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second') as hour,
    extract(DAY FROM timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second') as day,
    extract(WEEK FROM timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second') as week,
    extract(MONTH FROM timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second') as month,
    extract(YEAR FROM timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second') as year,
    extract(DAY FROM timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second') as weekday
FROM staging_events
""")

# List of SQL commands defined above
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
