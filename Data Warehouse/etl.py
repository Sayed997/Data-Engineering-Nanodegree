import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

""" The load_staging_tables function executes the copy table queries.
        Param1: Creates a cursor
        Param2: Creates a connection to the database 
        Cursor and connection needed to run queries
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        
""" The insert_tables function executes the insert table queries.
        Param1: Creates a cursor
        Param2: Creates a connection to the database 
        Cursor and connection needed to run queries
"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()