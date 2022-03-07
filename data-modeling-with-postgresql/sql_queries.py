# DENOTATIONS
drop_statement = 'drop table if exists'

# DROP TABLES
songplay_table_drop = f"{drop_statement} songplays"
user_table_drop = f"{drop_statement} users"
song_table_drop = f"{drop_statement} songs"
artist_table_drop = f"{drop_statement} artists"
time_table_drop = f"{drop_statement} time"

# CREATE TABLES

songplay_table_create = ("""
    create table if not exists songplays (
        songplay_id serial primary key, 
        start_time timestamp not null, 
        user_id int not null, 
        level varchar, 
        song_id varchar, 
        artist_id varchar, 
        session_id int, 
        location varchar, 
        user_agent varchar
    )
""")

user_table_create = ("""
    create table if not exists users (
        user_id int not null, 
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level varchar,
        primary key (user_id)
    )
""")

song_table_create = ("""
    create table if not exists songs (
        song_id varchar, 
        title varchar, 
        artist_id varchar, 
        year int, 
        duration float,
        primary key (song_id)
    )
""")

artist_table_create = ("""
    create table if not exists artists (
        artist_id varchar, 
        name varchar, 
        location varchar, 
        latitude float, 
        longitude float,
        primary key (artist_id)
    )
""")

time_table_create = ("""
    create table if not exists time (
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

# INSERT RECORDS

songplay_table_insert = ("""
    insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    values (%s, %s, %s, %s, %s, %s, %s, %s)
    on conflict (songplay_id) do nothing
""")

user_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    values (%s, %s, %s, %s, %s)
    on conflict (user_id) do update set level = excluded.level
""")

song_table_insert = ("""
    insert into songs (song_id, title, artist_id, year, duration)
    values (%s, %s, %s, %s, %s)
    on conflict (song_id) do nothing
""")

artist_table_insert = ("""
    insert into artists (artist_id, name, location, latitude, longitude)
    values (%s, %s, %s, %s, %s)
    on conflict (artist_id) do nothing
""")


time_table_insert = ("""
    insert into time (start_time, hour, day, week, month, year, weekday)
    values (%s, %s, %s, %s, %s, %s, %s)
    on conflict (start_time) do nothing
""")

# FIND SONGS

song_select = ("""
    SELECT s.song_id, a.artist_id FROM songs as s JOIN artists as a ON a.artist_id = s.artist_id WHERE s.title = %s AND a.name = %s AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]