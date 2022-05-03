import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    - Drop tables if exist. Queries are taken from `drop_table_queries` list.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print(f'{query} - DONE!')
        except Exception as e:
            print(e)


def create_tables(cur, conn):
    """
    - Create tables if not exist. Queries are taken from `create_table_queries` list.
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print(f'{query} - DONE!')
        except Exception as e:
            print(e)


def main():
    """
    - Establishes connection with the database and gets cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()