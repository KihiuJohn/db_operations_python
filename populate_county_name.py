# populate_county_name.py

import os
import sys
import logging
from sqlalchemy import create_engine, MetaData, Table, Column, String, Boolean, select, update
from sqlalchemy.orm import sessionmaker  # Ensure this import is present
from sqlalchemy.exc import SQLAlchemyError

def setup_logging():
    """
    Configures logging to output messages to both console and a log file.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler("populate_county_name.log"),
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

def add_county_name_column(engine, table_name):
    """
    Adds the 'county_name' column to the specified table if it does not exist.
    """
    metadata = MetaData()
    metadata.reflect(bind=engine)
    
    if table_name not in metadata.tables:
        logging.error(f"Table '{table_name}' does not exist in the database.")
        sys.exit(1)
    
    table = metadata.tables[table_name]
    
    if 'county_name' not in table.c:
        try:
            # Define the new column
            new_column = Column('county_name', String(255), nullable=True)
            new_column.create(table)
            logging.info(f"Added 'county_name' column to '{table_name}' table.")
        except SQLAlchemyError as e:
            logging.error(f"Error adding 'county_name' column: {e}")
            sys.exit(1)
    else:
        logging.info(f"'county_name' column already exists in '{table_name}' table.")

def define_location_to_county_mapping():
    """
    Defines and returns a dictionary mapping locations to their respective counties.
    """
    location_to_county = {
        "runda": "Nairobi",
        "loresho": "Nairobi",
        "kileleshwa": "Nairobi",
        "westlands": "Nairobi",
        "lavington": "Nairobi",
        "nyari": "Nairobi",
        "ridgeways": "Nairobi",
        "kitisuru": "Nairobi",
        "kilifi": "Kilifi",
        "rosslyn": "Nairobi",
        "karen": "Nairobi",
        "riverside": "Nairobi",
        "brookside": "Nairobi",
        "rosslyn lone tree": "Nairobi",
        "gigiri": "Nairobi",
        "ridgeway spring": "Nairobi",
        "kiambu": "Kiambu",
        "peponi": "Nairobi",
        "spring valley": "Nairobi",
        "lower kabete": "Nairobi",
        "eldama ravine": "Nairobi",
        "new muthaiga": "Nairobi",
        "kilimani": "Nairobi",
        "parklands": "Nairobi",
        "thigiri": "Nairobi",
        "parkland": "Nairobi",
        "muthaiga": "Nairobi",
        "mathenge": "Nairobi",
        "redhill": "Nairobi",
        "ridge ways": "Nairobi",
        "runda park": "Nairobi",
        "kitusuru": "Nairobi",
        "spring valley gated community": "Nairobi",
        "kyuna": "Nairobi",
        "red hill": "Nairobi",
        "muthaiga north": "Nairobi",
        "thigiri ridge": "Nairobi",
        "rosslyn red hill": "Nairobi",
        "two rivers rosslyn": "Nairobi",
        "limuru": "Kiambu",
        "general mathenge": "Nairobi",
        "kiambu runda": "Kiambu",
        "waiyaki way": "Nairobi"
    }
    return location_to_county

def update_county_name(engine, table_name, mapping):
    """
    Updates the 'county_name' field in the specified table based on the location-to-county mapping.
    """
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables[table_name]
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    total_updated = 0
    total_locations = len(mapping)
    unmapped_locations = []
    
    for location, county in mapping.items():
        try:
            # Perform a case-sensitive match on the 'location' field
            stmt = (
                update(table)
                .where(table.c.location == location)
                .values(county_name=county)
            )
            result = session.execute(stmt)
            session.commit()
            updated = result.rowcount
            total_updated += updated
            logging.info(f"Updated {updated} records for location '{location}' with county '{county}'.")
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"Error updating records for location '{location}': {e}")
    
    # Identify locations not in the mapping
    try:
        stmt_unmapped = select(table.c.location).where(table.c.county_name == None)
        results_unmapped = session.execute(stmt_unmapped).fetchall()
        for row in results_unmapped:
            unmapped_locations.append(row['location'])
        if unmapped_locations:
            logging.warning("The following locations were not mapped to any county:")
            for loc in unmapped_locations:
                logging.warning(f"- {loc}")
    except SQLAlchemyError as e:
        logging.error(f"Error fetching unmapped locations: {e}")
    
    logging.info(f"Total records updated: {total_updated} out of {total_locations} locations.")
    session.close()

def main():
    setup_logging()
    
    # Database connection details
    DB_USERNAME = 'root'        # Replace with your MySQL username
    DB_PASSWORD = ''            # Replace with your MySQL password
    DB_HOST = 'localhost'
    DB_PORT = '3306'            # Default MySQL port
    DB_NAME = 'archstone_test'  # Replace with your actual database name
    TABLE_NAME = 'properties_new'  # Table to be altered
    
    # Create database engine
    engine = create_db_engine(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    
    # Check and add 'county_name' column if necessary
    add_county_name_column(engine, TABLE_NAME)
    
    # Define the mapping from location to county
    location_to_county = define_location_to_county_mapping()
    
    # Update the 'county_name' field based on the mapping
    update_county_name(engine, TABLE_NAME, location_to_county)
    
    # Close the engine
    engine.dispose()
    logging.info("Database connection closed.")
    logging.info("Script completed successfully.")

if __name__ == "__main__":
    main()
