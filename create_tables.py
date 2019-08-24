import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn, queries):
    """
    Drop tables
    """
    for query in queries:
        print('Executing drop: '+query)
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn, queries):
    """
    Create staging and dimensional tables
    """
    for query in queries:
        print('Executing create: '+query)
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
        print('Dropping existing tables if any')
        drop_tables(cur, conn, drop_table_queries)
        print('Dropping existing tables if any SUCCESS')
    except Exception as e:
        print(e)
        print('Dropping existing tables if any FAILED')

    try:
        print('Creating tables')
        create_tables(cur, conn, create_table_queries)
        print('Creating tables SUCCESS')
    except Exception as e:
        print(e)
        print('Creating tables FAILED')

    conn.close()
    print('Create table Ended')


if __name__ == "__main__":
    main()
