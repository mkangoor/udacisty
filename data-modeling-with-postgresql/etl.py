import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    - read song files from /home/workspace/data/songs_data
    - filter the files according to requirements for song and artist tables
    - insert values to the tables
    '''
    # open song file
    df = pd.read_json(filepath,lines = True)

    # insert song record
    song_data = df.loc[:, ['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()
    cur.execute(song_table_insert, song_data.pop())
    
    # insert artist record
    artist_data = df.loc[:, ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()
    cur.execute(artist_table_insert, artist_data.pop())


def process_log_file(cur, filepath):
    '''
    - read log files from /home/workspace/data/log_data
    - perform filtering/transforming the files according to requirements for time, user and songplay tables
    - insert values to the tables
    '''
    # open log file
    df = pd.read_json(filepath,lines = True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong', :]

    # convert timestamp column to datetime
#     t = 
    df['ts_stand'] = pd.to_datetime(df['ts'], unit = 'ms')
    
    # insert time data records
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
    time_data_dict = {a : b for a,b in zip(column_labels, time_data)}
    time_df = pd.DataFrame(data = time_data_dict)

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print(e)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e:
            print(e)

    # insert songplay records
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
                index,
                row.ts, 
                row.userId, 
                row.level, 
                songid, 
                artistid, 
                row.sessionId, 
                row.location, 
                row.userAgent
            )
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print(e)


def process_data(cur, conn, filepath, func):
    '''
    - read all .json file for a given `filepath` directory
    - print count of files found in the directory
    - process files either by `process_song_file` or `process_log_file`
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    - create connection to the data base
    - process files either by `process_song_file` or `process_log_file`
    - close connection afterwards
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
