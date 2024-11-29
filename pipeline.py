from typing import Optional, Dict, Any
import sqlite3
import pandas as pd
import logging


def create_table(db_conn, table_name):
    cursor = db_conn.cursor()
    if table_name == "work_hours":
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS work_hours (
                employee_id INTEGER, 
                employee_name TEXT,
                date TEXT,
                project_id TEXT, 
                task_id TEXT,
                worked INTEGER,
                description TEXT,
                PRIMARY KEY (employee_id, date, task_id)
            );
        """)
    elif table_name == "time_off":
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS time_off (
                employee_id INTEGER, 
                employee_name TEXT,
                date_start TEXT,
                date_end TEXT,
                reason TEXT,
                PRIMARY KEY (employee_id, date_start, date_end)
            );
        """)
    db_conn.commit()

def connect_to_db(db_file: str) -> Optional[sqlite3.Connection]:
    """
    Function to connect to the database

    Parameters:
    db_file (str): the path to the database file

    Returns:
    sqlite3.Connection: the database connection, or None if an error occurred
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception as e:
        logging.error(f"Error occurred while connecting to the database {e}")
        return None

def clean_data(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Function to clean the data

    Parameters:
    df (pd.Dataframe): the dataframe to be cleaned

    Returns:
    pd.DataFrame: the cleaned dataframe, or None if an error occurred
    """
    try: 
        # Remove leading/trailing whitespaces from column names 
        df.columns = df.columns.str.strip()

        # Remove duplicate rows
        df.drop_duplicates(inplace=True)

        # Check for missing values
        if df.isnull().values.any():
            logging.info("Warning: The data contains missing values")

        return df
    except Exception as e:
        logging.error(f"Error occurred while cleaning the data {e}")
        return None


def load_csv_to_db(db_conn: sqlite3.Connection, table_name: str, csv_file: str, dtypes: Optional[Dict[str, Any]] = None) -> None:
    """
    Function to load a CSV file into a database table

    Parameters:
    db_conn (sqlite3.Connection): the database connection
    table_name (str): the name of the table to load the data into
    csv_file (str): the path to the CSV file
    dtypes (Dict[str, Any]): a dictionary of column names and data types to use when reading the CSV file

    Returns:
    None
    """
    try:
        try:
            if dtypes:
                df = pd.read_csv(csv_file, delimiter=';', dtype=dtypes)
            else:
                df = pd.read_csv(csv_file, delimiter=';')
        except pd.errors.ParserError as e:
            logging.error(f"File {csv_file} is not a valid CSV file.")
            return None
    
        # Clean the data
        df = clean_data(df)

        if df is not None:
            df.to_sql(table_name, db_conn, if_exists='replace', index=False)
        else:
            logging.error("Data could not be cleaned. Not loading data into database")
    except Exception as e:
        logging.error(f"Error occurred while loading the data to the database {e}")

def load_csv_to_db_in_chunks(db_conn: sqlite3.Connection, table_name: str, csv_file: str, chunksize: int = 1000) -> None:
    """
    Function to load a CSV file into a database table in chunks

    Parameters:
    db_conn (sqlite3.Connection): the database connection
    table_name (str): the name of the table to load the data into
    csv_file (str): the path to the CSV file
    chunksize (int): the number of rows to load at a time

    Returns:
    None
    """
    try:
        for chunk in pd.read_csv(csv_file, chunksize=chunksize):
            chunk.to_sql(table_name, db_conn, if_exists='append', index=False)
    except Exception as e:
        logging.error(f"Error occurred while loading the data to the database in chunks {e}")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("CSVtoDB").getOrCreate()

def load_csv_to_db_with_spark(csv_file, output_table, db_properties):
    """
    Function to load a CSV file into a database table using Spark

    Parameters:
    csv_file (str): the path to the CSV file
    output_table (str): the name of the table to load the data into
    db_properties (Dict[str, str]): a dictionary of database properties
    """
    df = spark.read.csv(csv_file, header=True, inferSchema=True)
    df.write.jdbc(url=db_properties['url'], table=output_table, mode='append', properties=db_properties)

def run_query_and_export_to_csv(db_conn: sqlite3.Connection, query: str, output_csv: str) -> None:
    """
    Function to run a query on the database and export the results to a CSV file

    Parameters:
    db_conn (sqlite3.Connection): the database connection
    query (str): the SQL query to run
    output_csv (str): the path to the CSV file to export the results to

    Returns:
    None
    """
    try:
        df = pd.read_sql_query(query, db_conn)
        df.to_csv(output_csv, index=False)
    except Exception as e:
        logging.error(f"Error occurred while running the query and exporting to CSV {e}")

def get_columns(db_conn, table_name):
    cursor = db_conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    elements = cursor.fetchall()
    return [(element[1], element[2]) for element in elements]

def add_primary_key(db_conn, old_table_name, new_table_name ,primary_key):
    cursor = db_conn.cursor()
    
    # Get the column names and types from the old table
    columns = get_columns(db_conn, old_table_name)

    # Create a string for the columns
    columns_str = ', '.join([f'{name} {type}' for name, type in columns])

    print(columns_str)

    # Create a new table with the primary key
    cursor.execute(f"""
        CREATE TABLE {new_table_name} (
            {columns_str},
            PRIMARY KEY ({primary_key})
        );
    """)

    # Copy the data from the old table to the new one
    cursor.execute(f"""
        INSERT INTO {new_table_name} SELECT * FROM {old_table_name};
    """)

    # Delete the old table
    cursor.execute(f"""
        DROP TABLE {old_table_name};
    """)

    # Rename the new table to the old table name
    cursor.execute(f"""
        ALTER TABLE {new_table_name} RENAME TO {old_table_name};
    """)

    db_conn.commit()


def main():
    """
    Function to load the data from the CSV files into the database
    """

    db_conn = connect_to_db('challenge.db')

    dtypes = {'project_id': str}

    # create_table(db_conn, 'work_hours')
    # create_table(db_conn, 'time_off')

    try: 
        # Load the data from the CSV files into the database
        load_csv_to_db(db_conn, 'work_hours', 'work_hours.csv', dtypes=dtypes)
        load_csv_to_db(db_conn, 'time_off', 'time_off.csv')

        # Define the SQL query to run
        query = """
            SELECT *
            FROM work_hours
            ORDER BY worked DESC;
        """

        output_csv = 'output.csv'
        run_query_and_export_to_csv(db_conn, query, output_csv)

        # add_primary_key(db_conn, 'work_hours', 'work_hours_new', 'employee_id, date, task_id')
        # add_primary_key(db_conn, 'time_off', 'time_off_new', 'employee_id, date_start, date_end')
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        # Close the database connection
        db_conn.close()

if __name__ == "__main__":
    main()