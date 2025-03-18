import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config['IAM_ROLE']['ARN']
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
                artist varchar(200) ,
                auth varchar(200) not null,
                firstname varchar(200) ,
                gender varchar(200) ,
                iteminsession integer not null,
                lastname varchar(200) ,
                length double precision ,
                level varchar(200) not null,
                location varchar(200) ,
                method varchar(20) not null,
                page varchar(200) not null,
                registration double precision ,
                sessionid integer not null,
                song varchar(200) ,
                status integer not null,
                ts BIGINT not null,         
                useragent varchar(512),
                userid varchar(200) 
                    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
                songid varchar(200),
                num_songs integer ,
                title varchar(200) not null,
                artist_name varchar(200) ,
                artist_latitude double precision ,
                year integer ,
                duration double precision not null,
                artist_id varchar(200) not null,
                artist_longitude double precision ,
                artist_location varchar(200) 
                    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
                songplay_id BIGINT IDENTITY(0,1) not null,
                start_time timestamp not null sortkey,
                user_id varchar(200) not null distkey,
                level varchar(20) not null,
                song_id varchar(200) ,
                artist_id varchar(200) ,
                session_id integer not null,
                location varchar(200) not null,
                user_agent VARCHAR(512) 
                    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
                user_id varchar(200) not null distkey,
                first_name varchar(200) ,
                last_name varchar(200) ,
                gender varchar(20)  sortkey,
                level varchar(200) not null
                );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
                song_id varchar(200) ,
                title varchar(200) not null sortkey,
                artist_id varchar(200),
                year integer ,
                duration DOUBLE PRECISION not null
                )
                diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
                artist_id varchar(200),
                name varchar(200)  sortkey,
                location varchar(200) ,
                latitude DOUBLE PRECISION ,
                longitude DOUBLE PRECISION 
                )
                diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
                start_time timestamp not null sortkey,
                hour integer not null,
                day integer not null,
                week integer not null,
                month integer not null,
                year integer not null,
                weekday integer not null
                )
                diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
from 's3://udacity-dend/log_data/'
credentials 'aws_iam_role={}'   
JSON 's3://udacity-dend/log_json_path.json'      
REGION 'us-west-2';                                                      
""").format(ARN)

staging_songs_copy = ("""
COPY staging_songs 
from 's3://udacity-dend/song_data/'
credentials 'aws_iam_role={}'    
JSON 'auto'                                                         
REGION 'us-west-2';
""").format(ARN)


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, 
                         artist_id, session_id, location, user_agent)
SELECT 
    TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS start_time,                          
    userid,
    level,
    staging_songs.songid AS song_id,
    staging_songs.artist_id,
    sessionid,
    staging_events.location,
    useragent
FROM staging_events
JOIN staging_songs 
    ON staging_events.song = staging_songs.title
    AND staging_events.artist = staging_songs.artist_name
WHERE staging_events.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id,first_name,last_name,gender,level)
SELECT distinct userid,firstname,lastname,gender,level
FROM
staging_events                                           
""")

song_table_insert = ("""
INSERT INTO songs (song_id,title,artist_id,year,duration)
SELECT distinct songid,title,artist_id,year,duration
FROM
staging_songs 
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,name,location,latitude,longitude)
SELECT distinct artist_id,artist_name,artist_location,artist_latitude,artist_longitude
FROM
staging_songs 
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)                     
SELECT DISTINCT 
    start_time,
    EXTRACT(HOUR FROM (start_time)) AS hour,
    EXTRACT(DAY FROM (start_time)) AS day,
    EXTRACT(WEEK FROM (start_time)) AS week,
    EXTRACT(MONTH FROM (start_time)) AS month,
    EXTRACT(YEAR FROM (start_time)) AS year,
    EXTRACT(DOW FROM (start_time)) AS weekday
FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert,songplay_table_insert, time_table_insert]

