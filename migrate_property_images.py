# migrate_property_images.py

import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, select, insert
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import sys
import logging
from urllib.parse import urlparse
import os

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
            logging.FileHandler("migrate_property_images.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

def extract_image_name(image_url):
    """
    Extracts the image name from a given image URL.

    Args:
        image_url (str): The URL of the image.

    Returns:
        str: The extracted image name.
    """
    try:
        path = urlparse(image_url).path
        image_name = os.path.basename(path)
        return image_name
    except Exception as e:
        logging.error(f"Error extracting image name from URL '{image_url}': {e}")
        return None

def main():
    # Setup logging
    setup_logging()

    # Database connection details
    DB_USERNAME = 'root'        # Replace with your MySQL username
    DB_PASSWORD = ''            # Replace with your MySQL password
    DB_HOST = 'localhost'
    DB_PORT = '3306'            # Default MySQL port
    DB_NAME = 'archstone_test'  # Updated database name

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
    source_table_name = 'property'          # Source table
    target_table_name = 'property_images'   # Target table

    # Check if tables exist
    if source_table_name not in metadata.tables:
        logging.error(f"Source table '{source_table_name}' does not exist in the database.")
        sys.exit(1)
    if target_table_name not in metadata.tables:
        logging.error(f"Target table '{target_table_name}' does not exist in the database.")
        sys.exit(1)

    # Access tables
    source_table = metadata.tables[source_table_name]
    target_table = metadata.tables[target_table_name]

    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()
    logging.info("Database session created.")

    # Define image URL columns in the source table
    image_columns = [
        'imgUrl',
        'imgroom1Url',
        'imgroom2Url',
        'imgroom3Url',
        'imgroom4Url',
        'imgroom5Url',
        'imgroom6Url',
        'imgroom7Url',
        'imgroom8Url'
    ]

    # Verify that all image columns exist in the source table
    for img_col in image_columns:
        if img_col not in source_table.c:
            logging.error(f"Column '{img_col}' does not exist in '{source_table_name}' table.")
            sys.exit(1)

    # Fetch all records from the source table
    try:
        stmt = select(source_table)
        results = session.execute(stmt).mappings().fetchall()
        logging.info(f"Fetched {len(results)} records from '{source_table_name}' table.")
    except Exception as e:
        logging.error(f"Error fetching data from '{source_table_name}': {e}")
        sys.exit(1)

    total_records = len(results)
    if total_records == 0:
        logging.warning(f"No records found in '{source_table_name}' table to migrate.")
        sys.exit(0)

    # Initialize a list to hold all new image records
    new_image_records = []

    # Determine the correct primary key column name
    # Replace 'ID' with the actual primary key column name if different
    primary_key_column = 'ID'
    if primary_key_column not in source_table.c:
        # If 'ID' does not exist, attempt to find the primary key
        primary_keys = [key.name for key in source_table.primary_key]
        if not primary_keys:
            logging.error(f"No primary key found in '{source_table_name}' table.")
            sys.exit(1)
        primary_key_column = primary_keys[0]
        logging.info(f"Primary key column determined as '{primary_key_column}'.")

    # Iterate through each property and collect image URLs
    for idx, record in enumerate(results, start=1):
        try:
            property_id = record[primary_key_column]
            for img_col in image_columns:
                image_url = record[img_col]
                if image_url and image_url.strip():  # Check if image_url is not None and not empty
                    image_name = extract_image_name(image_url)
                    if image_name:
                        new_image = {
                            'property_id': property_id,
                            'image_path': image_name,
                            'created_at': datetime.now(),
                            'updated_at': datetime.now()
                        }
                        new_image_records.append(new_image)
            if idx % 1000 == 0:
                logging.info(f"Processed {idx}/{total_records} properties.")
        except KeyError as ke:
            logging.error(f"KeyError for record {idx}: {ke}. Check if the primary key column '{primary_key_column}' exists.")
            continue
        except Exception as e:
            logging.error(f"Unexpected error processing record {idx}: {e}")
            continue

    logging.info(f"Total images to insert: {len(new_image_records)}")

    if not new_image_records:
        logging.warning("No image URLs found to migrate.")
        sys.exit(0)

    # Batch insert the new image records
    try:
        batch_size = 1000  # Adjust the batch size as needed
        for i in range(0, len(new_image_records), batch_size):
            batch = new_image_records[i:i + batch_size]
            session.execute(insert(target_table), batch)
            session.commit()
            logging.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} records.")
    except Exception as e:
        session.rollback()
        logging.error(f"Error inserting image records: {e}")
        sys.exit(1)

    # Close the session
    session.close()
    logging.info("Database session closed.")
    logging.info("Property images migration completed successfully.")

if __name__ == "__main__":
    main()
