# export_unique_locations.py

import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

def setup_logging():
    """
    Configures logging to output messages to both console and a log file.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler("export_unique_locations.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

def create_db_engine(username, password, host, port, database):
    """
    Creates and returns a SQLAlchemy engine.
    """
    try:
        db_url = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        engine = create_engine(db_url, echo=False)
        logging.info("Database engine created successfully.")
        return engine
    except Exception as e:
        logging.error(f"Error creating database engine: {e}")
        sys.exit(1)

def fetch_location_data(engine, table_name, column_name):
    """
    Fetches all data from the specified column in the given table.
    """
    try:
        query = f"SELECT `{column_name}` FROM `{table_name}`;"
        df = pd.read_sql(query, engine)
        logging.info(f"Fetched {len(df)} records from '{table_name}' table.")
        return df
    except SQLAlchemyError as e:
        logging.error(f"Error fetching data from '{table_name}' table: {e}")
        sys.exit(1)

def clean_location_data(df, column_name):
    """
    Normalizes and removes duplicate location names.
    """
    logging.info("Cleaning and normalizing location data...")
    # Drop rows where location is NULL or empty
    df = df.dropna(subset=[column_name])
    df = df[df[column_name].str.strip() != '']
    
    # Normalize: lowercase and strip whitespace
    df[column_name] = df[column_name].str.lower().str.strip()
    
    # Remove duplicates
    unique_locations = df[column_name].drop_duplicates().reset_index(drop=True)
    
    logging.info(f"Reduced to {len(unique_locations)} unique location names after cleaning.")
    return unique_locations

def export_to_csv(df, output_file):
    """
    Exports the DataFrame to a CSV file.
    """
    try:
        df.to_csv(output_file, index=False, header=['location'])
        logging.info(f"Successfully exported data to '{output_file}'.")
    except Exception as e:
        logging.error(f"Error exporting data to CSV: {e}")
        sys.exit(1)

def main():
    setup_logging()
    
    # Database connection details
    DB_USERNAME = 'root'        # Replace with your MySQL username
    DB_PASSWORD = ''            # Replace with your MySQL password
    DB_HOST = 'localhost'
    DB_PORT = '3306'            # Default MySQL port
    DB_NAME = 'archstone_test'  # Replace with your actual database name
    
    # Table and column details
    TABLE_NAME = 'properties_new'
    COLUMN_NAME = 'location'
    
    # Output CSV file
    OUTPUT_CSV = 'unique_locations.csv'
    
    # Create database engine
    engine = create_db_engine(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    
    # Fetch location data
    location_df = fetch_location_data(engine, TABLE_NAME, COLUMN_NAME)
    
    # Clean and deduplicate data
    unique_locations = clean_location_data(location_df, COLUMN_NAME)
    
    # Export to CSV
    export_to_csv(unique_locations, OUTPUT_CSV)
    
    # Close the engine
    engine.dispose()
    logging.info("Database connection closed.")
    logging.info("Script completed successfully.")

if __name__ == "__main__":
    main()
