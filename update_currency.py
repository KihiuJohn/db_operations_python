# update_currency.py

import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, select, update, and_
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
            logging.FileHandler("update_currency.log"),
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
    table_name = 'properties_new'    # Table to update

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

    # Define the price range
    price_min = 0
    price_max = 10000

    # Query records where price is between 0 and 10,000 inclusive
    try:
        stmt_buy = select(table).where(and_(table.c.price >= price_min, table.c.price <= price_max))
        records_buy = session.execute(stmt_buy).mappings().fetchall()
        logging.info(f"Fetched {len(records_buy)} records with price between {price_min} and {price_max}.")
    except Exception as e:
        logging.error(f"Error fetching 'buy' data from '{table_name}': {e}")
        sys.exit(1)

    # Query records where price is greater than 10,000
    try:
        stmt_rent = select(table).where(table.c.price > price_max)
        records_rent = session.execute(stmt_rent).mappings().fetchall()
        logging.info(f"Fetched {len(records_rent)} records with price greater than {price_max}.")
    except Exception as e:
        logging.error(f"Error fetching 'rent' data from '{table_name}': {e}")
        sys.exit(1)

    # Total records to update
    total_records_buy = len(records_buy)
    total_records_rent = len(records_rent)

    # Process 'buy' records
    for idx, record in enumerate(records_buy, start=1):
        try:
            property_id = record['id']
            original_currency = record['currency']

            # Skip if currency is already 'USD'
            if original_currency == 'USD':
                continue

            # Prepare update statement
            update_stmt = (
                update(table)
                .where(table.c.id == property_id)
                .values(
                    currency='USD',
                    updated_at=datetime.now()
                )
            )

            # Execute update
            session.execute(update_stmt)

            # Commit every 100 records or at the end
            if idx % 100 == 0 or idx == total_records_buy:
                session.commit()
                logging.info(f"Updated {idx}/{total_records_buy} 'buy' records to 'USD'.")

        except Exception as e:
            session.rollback()
            logging.error(f"Error updating 'buy' record {idx} (ID: {record['id']}): {e}")

    # Process 'rent' records
    for idx, record in enumerate(records_rent, start=1):
        try:
            property_id = record['id']
            original_currency = record['currency']

            # Skip if currency is already 'Ksh'
            if original_currency == 'Ksh':
                continue

            # Prepare update statement
            update_stmt = (
                update(table)
                .where(table.c.id == property_id)
                .values(
                    currency='Ksh',
                    updated_at=datetime.now()
                )
            )

            # Execute update
            session.execute(update_stmt)

            # Commit every 100 records or at the end
            if idx % 100 == 0 or idx == total_records_rent:
                session.commit()
                logging.info(f"Updated {idx}/{total_records_rent} 'rent' records to 'Ksh'.")

        except Exception as e:
            session.rollback()
            logging.error(f"Error updating 'rent' record {idx} (ID: {record['id']}): {e}")

    # Close the session
    session.close()
    logging.info("Database session closed.")
    logging.info("Currency column updated successfully.")

if __name__ == "__main__":
    main()
