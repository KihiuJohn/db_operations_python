# organize_property_images.py

import os
import sys
import shutil
import logging
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.orm import sessionmaker

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
            logging.FileHandler("organize_property_images.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

def main():
    # Setup logging
    setup_logging()

    # Database connection details
    DB_USERNAME = 'root'        # Replace with your MySQL username
    DB_PASSWORD = ''            # Replace with your MySQL password
    DB_HOST = 'localhost'
    DB_PORT = '3306'            # Default MySQL port
    DB_NAME = 'archstone_test'  # Replace with your actual database name

    # Source and destination directories
    SOURCE_DIR = 'source_images'  # Directory where source images are stored
    DEST_DIR = 'uploads'          # Destination directory for organized images

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

    # Define table name
    table_name = 'property_images'

    # Check if table exists
    if table_name not in metadata.tables:
        logging.error(f"Table '{table_name}' does not exist in the database.")
        sys.exit(1)

    # Access table
    table = metadata.tables[table_name]

    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()
    logging.info("Database session created.")

    # Fetch all records from the table
    try:
        stmt = select(table.c.property_id, table.c.image_path)
        results = session.execute(stmt).fetchall()
        logging.info(f"Fetched {len(results)} records from '{table_name}' table.")
    except Exception as e:
        logging.error(f"Error fetching data from '{table_name}': {e}")
        sys.exit(1)

    if not results:
        logging.warning(f"No records found in '{table_name}' table.")
        sys.exit(0)

    # Organize images by property_id
    images_by_property = {}
    for property_id, image_path in results:
        if property_id not in images_by_property:
            images_by_property[property_id] = []
        images_by_property[property_id].append(image_path)

    # Create the uploads directory if it doesn't exist
    if not os.path.exists(DEST_DIR):
        os.makedirs(DEST_DIR)
        logging.info(f"Created directory '{DEST_DIR}'.")

    # Process each property_id
    for property_id, image_paths in images_by_property.items():
        property_dir = os.path.join(DEST_DIR, str(property_id))
        os.makedirs(property_dir, exist_ok=True)
        logging.info(f"Processing property_id {property_id} with {len(image_paths)} images.")

        for image_name in image_paths:
            source_image_path = os.path.join(SOURCE_DIR, image_name)
            destination_image_path = os.path.join(property_dir, image_name)

            if os.path.exists(source_image_path):
                try:
                    shutil.copy2(source_image_path, destination_image_path)
                    logging.info(f"Copied '{image_name}' to '{property_dir}'.")
                except Exception as e:
                    logging.error(f"Error copying '{image_name}': {e}")
            else:
                logging.warning(f"Source image '{image_name}' does not exist in '{SOURCE_DIR}'.")

    # Close the session
    session.close()
    logging.info("Database session closed.")
    logging.info("Image files organized successfully.")

if __name__ == "__main__":
    main()
