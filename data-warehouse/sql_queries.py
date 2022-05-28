import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')
LOG_JSON_PATH = config.get('S3','LOG_JSONPATH')
IAM_ROLE = config.get('IAM_ROLE','ARN')

# DROP TABLES
drop_statement = 'DROP TABLE IF EXISTS'

staging_events_table_drop = f'{drop_statement} staging_events'
staging_songs_table_drop = f'{drop_statement} staging_songs'
songplay_table_drop = f'{drop_statement} songplay'
user_table_drop = f'{drop_statement} users'
song_table_drop = f'{drop_statement} songs'
artist_table_drop = f'{drop_statement} artists'
time_table_drop = f'{drop_statement} time'

# CREATE TABLES

staging_events_table_create = ("""
   CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar,
        first_name varchar,
        gender varchar,
        item_in_session int,
        last_name varchar,
        length float,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration bigint,
        session_id int,
        song varchar,
        status varchar,
        ts bigint,
        user_agent varchar,
        user_id int
    )
""")

staging_songs_table_create = ("""
   CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int,
        artist_id varchar,
        artist_latitude float,
        artist_longitude float,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year int
    )
""")

songplay_table_create = ("""
   CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int identity(0,1), 
        start_time bigint not null, 
        user_id int not null, 
        level varchar, 
        song_id varchar, 
        artist_id varchar, 
        session_id int, 
        location varchar, 
        user_agent varchar,
        primary key (songplay_id)
    )
""")

user_table_create = ("""
   CREATE TABLE IF NOT EXISTS users (
        user_id int, 
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level varchar,
        primary key (user_id)
    )
""")

song_table_create = ("""
   CREATE TABLE IF NOT EXISTS songs (
        song_id varchar, 
        title varchar, 
        artist_id varchar, 
        year int, 
        duration float,
        primary key (song_id)
    )
""")

artist_table_create = ("""
   CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar, 
        name varchar, 
        location varchar, 
        latitude float, 
        longitude float,
        primary key (artist_id)
    )
""")

time_table_create = ("""
   CREATE TABLE IF NOT EXISTS time (
        start_time bigint not null, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday int,
        primary key (start_time)
    )
""")

# STAGING TABLES

staging_events_copy = (f"""
    COPY staging_events FROM {LOG_DATA} 
    credentials 'aws_iam_role={IAM_ROLE}'
    format as json {LOG_JSON_PATH} 
    region 'us-west-2';
""")

staging_songs_copy = (f"""
    copy staging_songs from {SONG_DATA} 
    credentials 'aws_iam_role={IAM_ROLE}'
    format as json 'auto'
    region 'us-west-2';
""")

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        ste.ts, 
        ste.user_id, 
        ste.level, 
        s.song_id, 
        a.artist_id, 
        ste.session_id, 
        ste.location, 
        ste.user_agent
    FROM staging_events ste
        INNER JOIN songs s ON ste.song = s.title
        INNER JOIN artists a ON s.artist_id = a.artist_id
    WHERE ste.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    FROM staging_events
    WHERE page='NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT 
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT 
        artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude
    FROM staging_songs
""")

TO_TIMESTAMP: str = "TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second'"

time_table_insert = (f"""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT 
        ts, 
        EXTRACT(hour FROM ({TO_TIMESTAMP})) AS hour, 
        EXTRACT(day FROM ({TO_TIMESTAMP})) AS day, 
        EXTRACT(week FROM ({TO_TIMESTAMP})) AS week, 
        EXTRACT(month FROM ({TO_TIMESTAMP})) AS month, 
        EXTRACT(year FROM ({TO_TIMESTAMP})) AS year, 
        EXTRACT(weekday FROM ({TO_TIMESTAMP})) AS weekday
    FROM staging_events
    WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
table_names = ['songplays', 'users', 'songs', 'artists', 'time']
