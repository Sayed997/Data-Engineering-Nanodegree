import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

""" The drop_tables function executes the drop table queries.
        Param1: Creates a cursor
        Param2: Creates a connection to the database 
        Cursor and connection needed to run queries
"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
""" The create_tables function executes the create table queries
        Param1: Creates a cursor
        Param2: Creates a connection to the database
        Cursor and connection needed to run queries
"""
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

""" - The main function reads the 'dwh.cfg' file and extracts its credential values in order to connect to the database.
    - Calls above functions and closes the connection.
"""
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()