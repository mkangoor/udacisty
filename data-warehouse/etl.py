import pandas as pd
import configparser
import psycopg2
from sql_queries import *
        
def load_staging_tables(cur, conn):
    """
    Loading data from S3 bucket to Redshift tables using Postgres.
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print(f'{query} - DONE!')
        except Exception as e:
            print(e)
            
def process_song_data(df: pd.DataFrame(), conn: psycopg2.extensions.connection, cur: psycopg2.extensions.cursor) -> None:
    """
    Loading data to songs and artists tables.
    """
    song_data = df.loc[:, ['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()
    artist_data = df.loc[:, ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()
    
    for i,j,k in zip([song_table_insert, artist_table_insert], [song_data, artist_data], [['songs', 1], ['artists', 2]]):
        try:
            print(f'Processing {k[0]} table ({k[1]}/5)\n')
            print()
            cur.executemany(i, j)
            print(f'{i} - QUERY IS PROCESSED!\n')
        except Exception as e:
            print(e)
            
    conn.commit()
    
def process_event_data(df: pd.DataFrame(), conn: psycopg2.extensions.connection, cur: psycopg2.extensions.cursor) -> None:
    """
    Loading data to time, users and songplays tables.
    """
    df['ts_stand'] = pd.to_datetime(df['ts'], unit = 'ms')
    
    # load time table
    time_data = [
        df['ts'],
        df['ts_stand'].dt.hour,
        df['ts_stand'].dt.day,
        df['ts_stand'].dt.weekofyear,
        df['ts_stand'].dt.month,
        df['ts_stand'].dt.year,
        df['ts_stand'].dt.weekday
    ]

    column_labels = ['timestamp', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday']
    time_data_dict = {a: b for a, b in zip(column_labels, time_data)}
    time_df = pd.DataFrame(data = time_data_dict)

    count = 0
    print('Procesing time table (3/5)\n')
    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
            count += 1
            if count % 1000 == 0:
                print(f'{count}/{time_df.shape[0]} rows is processed.')
        except psycopg2.Error as e:
            print(e)
    print(f'{time_table_insert} - DONE!')
            
    user_df = df[['user_id', 'first_name', 'last_name', 'gender', 'level']].drop_duplicates()

    # load users table
    print('Procesing user table (4/5)\n')
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, list(row))
        except psycopg2.Error as e:
            print(e)
    print(f'{user_table_insert} - DONE!')
            
    # load songplays table
    count = 0
    print('Procesing songplay table (5/5)\n')
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()

            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            # insert songplay record
            songplay_data = (
                row.ts_stand,
                row.user_id,
                row.level,
                songid,
                artistid,
                row.session_id,
                row.location,
                row.user_agent
            )
            cur.execute(songplay_table_insert, songplay_data)
            count += 1
            if count % 1000 == 0:
                print(f'{count}/{songplay_data.shape[0]} rows is processed.')
        except psycopg2.Error as e:
            print(e)
    print(f'{songplay_table_insert} - DONE!')
    
    conn.commit()


def main():
    """
    - Read config file
    - Establish connection to Redshift cluster
    - Get data from staging tables
    - Process song data
    - Proces event data
    - Close connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    
    events_df = pd.read_sql(staging_events_select, conn)
    songs_df = pd.read_sql(staging_songs_select, conn)

    process_song_data(songs_df, conn, cur)
    process_event_data(events_df, conn, cur)

    conn.close()


if __name__ == "__main__":
    main()