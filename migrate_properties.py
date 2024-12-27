# migrate_properties.py

import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, select, insert
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import sys
import logging
import re

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
            logging.FileHandler("migration.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

def extract_numeric_price(price_str):
    """
    Extracts the first occurrence of a numeric value from a price string.
    Removes commas and ignores any non-numeric characters.
    
    Args:
        price_str (str): The raw price string from the old table.
    
    Returns:
        float or None: The extracted numeric price, or None if extraction fails.
    """
    if not price_str:
        return None

    # Remove commas to handle thousand separators
    price_str_clean = price_str.replace(',', '')

    # Use regex to find the first numeric value (integer or decimal)
    match = re.search(r'\d+\.?\d*', price_str_clean)
    if match:
        try:
            return float(match.group())
        except ValueError:
            logging.warning(f"Unable to convert extracted price to float: '{match.group()}' from original '{price_str}'")
            return None
    else:
        logging.warning(f"No numeric value found in price string: '{price_str}'")
        return None

def word_to_num(word):
    """
    Converts a word representation of a number to its integer equivalent.
    
    Args:
        word (str): The word representing the number (e.g., 'four').
    
    Returns:
        int or None: The integer equivalent, or None if the word is not a valid number.
    """
    word = word.lower()
    numbers = {
        'zero': 0,
        'one': 1,
        'two': 2,
        'three':3,
        'four':4,
        'five':5,
        'six':6,
        'seven':7,
        'eight':8,
        'nine':9,
        'ten':10,
        'eleven':11,
        'twelve':12,
        'thirteen':13,
        'fourteen':14,
        'fifteen':15,
        'sixteen':16,
        'seventeen':17,
        'eighteen':18,
        'nineteen':19,
        'twenty':20
        # Extend this dictionary as needed
    }
    return numbers.get(word, None)

def extract_numeric_rooms(rooms_str, propertytype):
    """
    Extracts the numeric value from the rooms string.
    Converts word numbers to integers and ignores properties of type 'Plot'.
    
    Args:
        rooms_str (str): The raw rooms string from the old table.
        propertytype (str): The property type to check for exclusion.
    
    Returns:
        int or None: The extracted numeric bedrooms, or None if extraction fails or propertytype is 'Plot'.
    """
    if not rooms_str:
        return None

    # Exclude properties of type 'Plot'
    if propertytype.lower() == 'plot':
        return None

    # First, try to extract digits
    match = re.search(r'\d+', rooms_str)
    if match:
        try:
            return int(match.group())
        except ValueError:
            logging.warning(f"Unable to convert extracted rooms to int: '{match.group()}' from original '{rooms_str}'")
            return None

    # If no digits, try to extract word number
    words = rooms_str.split()
    for word in words:
        num = word_to_num(word)
        if num is not None:
            return num

    # If no valid number found
    logging.warning(f"No numeric value found in rooms string: '{rooms_str}'")
    return None

def main():
    # Setup logging
    setup_logging()

    # Database connection details
    DB_USERNAME = 'root'        # Default XAMPP MySQL username
    DB_PASSWORD = ''            # Default XAMPP MySQL password is empty
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
    old_table_name = 'property'          # Old table
    new_table_name = 'properties_new'    # New table
    images_table_name = 'property_images' # Images table

    # Check if tables exist
    missing_tables = []
    for table_name in [old_table_name, new_table_name, images_table_name]:
        if table_name not in metadata.tables:
            missing_tables.append(table_name)
    if missing_tables:
        logging.error(f"Missing tables in the database: {', '.join(missing_tables)}")
        sys.exit(1)

    # Access tables
    old_table = metadata.tables[old_table_name]
    new_table = metadata.tables[new_table_name]
    images_table = metadata.tables[images_table_name]

    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()
    logging.info("Database session created.")

    # Query all records from old_table using mappings()
    try:
        stmt = select(old_table)
        results = session.execute(stmt).mappings().fetchall()
        logging.info(f"Fetched {len(results)} records from '{old_table_name}' table.")
    except Exception as e:
        logging.error(f"Error fetching data from '{old_table_name}': {e}")
        sys.exit(1)

    total_records = len(results)
    if total_records == 0:
        logging.warning(f"No records found in '{old_table_name}' table to migrate.")
        sys.exit(0)

    # Process each record
    for idx, record in enumerate(results, start=1):
        try:
            # Extract data from old record
            old_id = record['ID']
            imgUrl = record['imgUrl']
            description = record['Description']
            rooms = record['rooms']
            location = record['location']
            price_str = record['Price']
            category = record['Category']
            propertytype = record['propertytype']

            # Prepare data for properties_new
            bedrooms = extract_numeric_rooms(rooms, propertytype)
            price = extract_numeric_price(price_str)

            new_prop = {
                'id': old_id,                       # Map ID to id
                'location': location,               # Map location
                'specific_location': record['Product'] if record['Product'] else None, # Map Product to specific_location
                'county_name': None,                # Unmapped, set to NULL
                'property_type': category if category else None,         # Map Category to property_type
                'property_category': propertytype if propertytype else None, # Map propertytype to property_category
                'description': description if description else None,      # Map Description
                'bedrooms': bedrooms,                                       # Mapped bedrooms
                'bathrooms': None,                # Unmapped, set to NULL
                'currency': None,                 # Unmapped, set to NULL
                'price': price,                   # Mapped price
                'amenities': None,                # Unmapped, set to NULL
                'is_featured': False,             # Ignore status, set to default FALSE
                'created_at': datetime.now(),     # Set to current timestamp
                'updated_at': datetime.now()      # Set to current timestamp
            }

            # Insert into properties_new
            insert_stmt = insert(new_table).values(new_prop)
            session.execute(insert_stmt)

            # Handle image URLs
            image_fields = [
                imgUrl,
                record.get('imgroom1Url'),
                record.get('imgroom2Url'),
                record.get('imgroom3Url'),
                record.get('imgroom4Url'),
                record.get('imgroom5Url'),
                record.get('imgroom6Url'),
                record.get('imgroom7Url'),
                record.get('imgroom8Url')
            ]

            image_records = []
            for img_url in image_fields:
                if img_url and img_url.strip():
                    image_records.append({
                        'property_id': old_id,       # Reference to the same ID
                        'image_path': img_url.strip()
                    })

            if image_records:
                session.execute(insert(images_table), image_records)

            # Commit every 100 records to manage transaction size
            if idx % 100 == 0 or idx == total_records:
                session.commit()
                logging.info(f"Migrated {idx}/{total_records} records.")

        except Exception as e:
            session.rollback()
            logging.error(f"Error migrating record {idx} (ID: {record['ID']}): {e}")

    # Close the session
    session.close()
    logging.info("Database session closed.")
    logging.info("Migration completed successfully.")

if __name__ == "__main__":
    main()
