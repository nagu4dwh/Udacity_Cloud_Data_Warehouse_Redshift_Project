import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#CREATE THE dist SCHEMA for DISTRIBUTION STRATEGY
create_dist_schema = "CREATE SCHEMA IF NOT EXISTS dist;"
set_search_path = "SET search_path TO dist;"

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS event_data"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_data"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"
dist_schema_drop = "DROP SCHEMA IF EXISTS dist CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
                CREATE TABLE IF NOT EXISTS event_data (
                                artist VARCHAR(MAX),
                                auth VARCHAR,
                                firstName VARCHAR,
                                gender VARCHAR,
                                itemInSession INTEGER,
                                lastName VARCHAR,
                                length DECIMAL,
                                level VARCHAR,
                                location VARCHAR,
                                method VARCHAR,
                                page VARCHAR,
                                registration DOUBLE PRECISION,
                                sessionId INTEGER,
                                song VARCHAR,
                                status INTEGER,
                                ts TIMESTAMP,
                                userAgent VARCHAR,
                                userId VARCHAR
    )
""")

staging_songs_table_create = ("""
                CREATE TABLE IF NOT EXISTS song_data (
                                artist_id VARCHAR,
                                artist_latitude DECIMAL,
                                artist_location VARCHAR,
                                artist_longitude DECIMAL,
                                artist_name VARCHAR(MAX),
                                duration DECIMAL,
                                num_songs SMALLINT,
                                song_id VARCHAR,
                                title VARCHAR,
                                year SMALLINT
    )
""")

songplay_table_create = ("""
                CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id VARCHAR NOT NULL,
                                start_time TIMESTAMP NOT NULL,
                                user_id VARCHAR NOT NULL DISTKEY,
                                level VARCHAR,
                                song_id VARCHAR NOT NULL SORTKEY,
                                artist_id VARCHAR NOT NULL,
                                session_id VARCHAR,
                                location VARCHAR,
                                user_agent VARCHAR
                                )
                          
""")

user_table_create = ("""
                CREATE TABLE IF NOT EXISTS users (
                                user_id VARCHAR NOT NULL SORTKEY DISTKEY,
                                first_name VARCHAR,
                                last_name VARCHAR,
                                gender CHAR,
                                level VARCHAR
                                )
                             
""")

song_table_create = ("""
                CREATE TABLE IF NOT EXISTS songs (
                                song_id VARCHAR NOT NULL SORTKEY,
                                title VARCHAR NOT NULL,
                                artist_id VARCHAR NOT NULL,
                                year INTEGER,
                                location VARCHAR                      
                                )
                                diststyle all;
""")

artist_table_create = ("""
                CREATE TABLE IF NOT EXISTS artists (
                                artist_id VARCHAR NOT NULL SORTKEY,
                                name VARCHAR NOT NULL,
                                location VARCHAR,
                                lattitude DECIMAL,
                                longitude DECIMAL                       
                                )
                                diststyle all;
""")

time_table_create = ("""
                CREATE TABLE IF NOT EXISTS time (
                                start_time TIMESTAMP NOT NULL SORTKEY,
                                hour SMALLINT,
                                day SMALLINT,
                                week SMALLINT,
                                month SMALLINT,
                                year SMALLINT,
                                weekday SMALLINT                
                                )
                                diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
                COPY dist.event_data FROM {}
                CREDENTIALS 'aws_iam_role={}'
                REGION 'us-west-2'
                TIMEFORMAT AS 'epochmillisecs'
                FORMAT AS JSON {};
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
                COPY dist.song_data FROM {}
                CREDENTIALS 'aws_iam_role={}'
                REGION 'us-west-2'
                FORMAT AS JSON 'auto'
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
                INSERT INTO dist.songplays(songplay_id,start_time, user_id,level,song_id,artist_id,session_id,location,user_agent)
                (SELECT DISTINCT sd.song_id,ed.ts,ed.userid,ed.level,sd.song_id,sd.artist_id,ed.sessionid,ed.location,ed.useragent
                FROM dist.song_data sd,dist.event_data ed
                WHERE sd.artist_name=ed.artist
                AND sd.title=ed.song
                AND ed.page = 'NextSong')
""")

user_table_insert = ("""
                INSERT INTO dist.users (user_id,first_name,last_name,gender,level)
                (SELECT DISTINCT userid,firstname,lastname,gender,level FROM dist.event_data);
""")

song_table_insert = ("""
                INSERT INTO dist.songs(song_id,title,artist_id,year,location)
                (SELECT DISTINCT song_id,title,artist_id,year,artist_location FROM dist.song_data)
""")

artist_table_insert = ("""
                INSERT INTO dist.artists (artist_id,name,location,lattitude,longitude)
                (SELECT DISTINCT artist_id,artist_name,artist_location,artist_latitude,artist_longitude FROM dist.song_data)
""")

time_table_insert = ("""
                INSERT INTO dist.time (start_time,hour,day,week,month,year,weekday)
                (select
                    ts as start_time,
                    extract(h from ts) as hour,
                    extract(d from ts) as day,
                    extract(w from ts) as week,
                    extract(mon from ts) as month,
                    extract(yr from ts) as year,
                    extract(dow from ts) as weekday
                    from dist.event_data)
""")

# QUERY LISTS

create_table_queries = [create_dist_schema,set_search_path,staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop,dist_schema_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert,song_table_insert,artist_table_insert,time_table_insert,songplay_table_insert]
