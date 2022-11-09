import psycopg2
import configparser
from sql_queries import create_table_queries, drop_table_queries
from sqlalchemy import create_engine


def create_database(db_prop):
    """Creates database.
    Connects to database. Drops existing and recreates database.
    """
    user = db_prop['username']
    password = db_prop['password']
    dbname = db_prop['dbname']

    # connect to default database
    try:
        conn.close()
    except:
        pass

    try:
        engine = create_engine(f'postgresql://{user}:{password}@127.0.0.1:5432/dummy')
    except:
        print("Could not connect to dummy Database - maybe it was not created?")

    print("helllo!")
    conn = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
    # create sparkify database with UTF8 encoding
    conn.execute("DROP DATABASE IF EXISTS world;")
    conn.execute("CREATE DATABASE world WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    engine = create_engine(f'postgresql://{user}:{password}@127.0.0.1:5432/{dbname}')

    return engine


def drop_tables(conn):
    """Drops tables.
    Drops all tables using pre-defined drop table queries.
    """
    for query in drop_table_queries:
        conn.execute(query)


def create_tables(conn):
    """Creates tables
    Creates all tables using pre-defined drop table queries.
    """
    for query in create_table_queries:
        conn.execute(query)


def main():
    """ETLs main data structures creataion.
    Drops and recreates all tables and databases.
    """

    try:
        conn.close()
    except:
        pass

    config = configparser.ConfigParser()
    config.read('../config_local.cfg')
    input_data = config['PATH']['COMMODITIES_DATA']

    db_prop = config['POSTGRESQL']

    engine = create_database(db_prop)
    conn = engine.connect()

    drop_tables(conn)
    create_tables(conn)
    conn.close()


if __name__ == "__main__":
    main()