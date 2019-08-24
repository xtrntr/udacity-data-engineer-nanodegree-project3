import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn, queries):
    """
    Load data from files stored in S3 -> to Redshift staging tables
    """
    for query in queries:
        print('Loading data by: '+query)
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn, queries):
    """
    Transform data from staging tables -> into dimensional tables
    """
    for query in queries:
        print('Transform data by: '+query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to redshift')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to redshift')
    cur = conn.cursor()

    try:
        print('Loading staging tables')
        load_staging_tables(cur, conn, copy_table_queries)
        print('Loading staging tables SUCCESS')
    except Exception as e:
        print(e)
        print('Loading staging tables FAILED')

    try:
        print('Transform from staging')
        insert_tables(cur, conn, insert_table_queries)
        print('Transform from staging SUCCESS')
    except Exception as e:
        print(e)
        print('Transform from staging FAILED')

    conn.close()
    print('ETL Ended')


if __name__ == "__main__":
    main()
