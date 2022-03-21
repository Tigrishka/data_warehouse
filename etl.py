import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Description: this function loads data from S3 into staging_events_copy and staging_songs_copy tables on Redshift.
    
    Arguments:
     cur: the cursor object. 
     conn: connection to sparkify database. 
    
    Returns:
     None
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Description: this function processes data into your analytics tables on Redshift.
    
    Arguments:
     cur: the cursor object. 
     conn: connection to sparkify database. 
    
    Returns:
     None
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()