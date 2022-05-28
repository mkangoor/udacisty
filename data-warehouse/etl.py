import pandas as pd
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, table_names
        
def load_staging_tables(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection) -> None:
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
            
def insert_rows_into_tables(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection) -> None:
    """
    Loading data from staging tables into final tables tables using Postgres.
    """
    for query, i in zip(insert_table_queries, table_names):
        try:
            cur.execute(query)
            conn.commit()
            print(f'{i} - DONE!')
        except Exception as e:
            print(e)

def main():
    """
    - Read config file
    - Establish connection to Redshift cluster
    - Get data from staging tables
    - Process data
    - Close connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_rows_into_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
