# update_property_type.py

import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, select, update
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import sys
import logging

def create_db_url(username, password, host, port, database):
    """
    Constructs the database URL for SQLAlchemy.
    """
    return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"

def setup_logging():
    """
    Configures logging to log messages to a file and the console.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler("update_property_type.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

def determine_property_type(original_type):
    """
    Determines the standardized property type ('buy' or 'rent') based on the original property_type text.

    Args:
        original_type (str): The original property_type text from the table.

    Returns:
        str or None: Returns 'buy' or 'rent' based on the analysis, or None if no match is found.
    """
    if not original_type:
        return None

    original_type_clean = original_type.strip()

    # Keywords for 'buy' and 'rent' mapping
    buy_keywords = [
        'For Sale', 'FOR SALE', 'for sale',
        'Plot for sale', 'Plot For Sale', 'plot for sale',
        'Sale', 'SALE',
        'Buy', 'BUY', 'buy',
        'Purchase', 'PURCHASE', 'purchase'
    ]

    rent_keywords = [
        'Rental', 'RENTAL', 'rental',
        'Rentals', 'RENTALS', 'rentals',
        'Rent', 'RENT', 'rent'
    ]

    # Determine new property_type based on keywords
    if original_type_clean in buy_keywords:
        logging.info(f"Matched '{original_type_clean}' to 'buy'")
        return 'buy'
    elif original_type_clean in rent_keywords:
        logging.info(f"Matched '{original_type_clean}' to 'rent'")
        return 'rent'

    logging.warning(f"No matching keyword found for property_type: '{original_type_clean}'")
    return None

def main():
    # Setup logging
    setup_logging()

    # Database connection details
    DB_USERNAME = 'root'        # Replace with your MySQL username
    DB_PASSWORD = ''            # Replace with your MySQL password
    DB_HOST = 'localhost'
    DB_PORT = '3306'            # Default MySQL port
    DB_NAME = 'archstone_test'     # Replace with your actual database name

    # Create database URL
    DATABASE_URL = create_db_url(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

    # Create SQLAlchemy engine
    try:
        engine = create_engine(DATABASE_URL, echo=False)
        logging.info("Database engine created successfully.")
    except Exception as e:
        logging.error(f"Error creating engine: {e}")
        sys.exit(1)

    # Initialize MetaData
    metadata = MetaData()

    # Reflect existing tables
    try:
        metadata.reflect(bind=engine)
        logging.info("Database schema reflected successfully.")
    except Exception as e:
        logging.error(f"Error reflecting metadata: {e}")
        sys.exit(1)

    # Define table names
    new_table_name = 'properties_new'    # New table

    # Check if table exists
    if new_table_name not in metadata.tables:
        logging.error(f"Table '{new_table_name}' does not exist in the database.")
        sys.exit(1)

    # Access table
    new_table = metadata.tables[new_table_name]

    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()
    logging.info("Database session created.")

    # Query all records from new_table using mappings()
    try:
        stmt = select(new_table)
        results = session.execute(stmt).mappings().fetchall()
        logging.info(f"Fetched {len(results)} records from '{new_table_name}' table.")
    except Exception as e:
        logging.error(f"Error fetching data from '{new_table_name}': {e}")
        sys.exit(1)

    total_records = len(results)
    if total_records == 0:
        logging.warning(f"No records found in '{new_table_name}' table to update.")
        sys.exit(0)

    # Process each record
    for idx, record in enumerate(results, start=1):
        try:
            # Extract data from current record
            property_id = record['id']
            original_property_type = record['property_type']

            # Determine the new property_type
            new_property_type = determine_property_type(original_property_type)

            # Compare only if original_property_type is not None
            if original_property_type:
                original_property_type_clean = original_property_type.strip()
                if new_property_type and new_property_type == original_property_type_clean.lower():
                    continue

            # If new_property_type is None, decide whether to skip or set to NULL
            if new_property_type is None:
                # Option 1: Skip updating
                # continue

                # Option 2: Set to NULL
                update_values = {
                    'property_type': None,
                    'updated_at': datetime.now()
                }
            else:
                update_values = {
                    'property_type': new_property_type,
                    'updated_at': datetime.now()
                }

            # Prepare update statement
            update_stmt = (
                update(new_table)
                .where(new_table.c.id == property_id)
                .values(**update_values)
            )

            # Execute update
            session.execute(update_stmt)

            # Commit every 100 records or at the end
            if idx % 100 == 0 or idx == total_records:
                session.commit()
                logging.info(f"Updated {idx}/{total_records} records.")

        except AttributeError as ae:
            # Handle cases where original_property_type is None
            logging.error(f"AttributeError for record {idx} (ID: {record['id']}): {ae}")
            continue
        except Exception as e:
            session.rollback()
            logging.error(f"Error updating record {idx} (ID: {record['id']}): {e}")

    # Close the session
    session.close()
    logging.info("Database session closed.")
    logging.info("Property type fields updated successfully.")

if __name__ == "__main__":
    main()
