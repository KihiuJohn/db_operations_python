import sys
from sqlalchemy import create_engine, MetaData, Table, select, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# ---------------------- Configuration ----------------------

# Database configuration - replace with your actual credentials
DB_USER = 'root'
DB_PASSWORD = ''
DB_HOST = 'localhost'        # e.g., 'localhost' or '127.0.0.1'
DB_PORT = '3306'                # Default MySQL port
DB_NAME = 'archstone_test'

# Choose your MySQL driver: 'mysqlconnector' or 'pymysql'
# Ensure you have installed the corresponding package:
# For 'mysqlconnector': pip install mysql-connector-python
# For 'pymysql': pip install pymysql
DRIVER = 'mysqlconnector'  # or 'pymysql'

# Create the database URL based on the chosen driver
if DRIVER == 'mysqlconnector':
    DATABASE_URL = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
elif DRIVER == 'pymysql':
    DATABASE_URL = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
else:
    print("Unsupported DRIVER specified. Choose 'mysqlconnector' or 'pymysql'.")
    sys.exit(1)

# ---------------------- Initialize Engine and Session ----------------------

# Initialize the database engine
engine = create_engine(DATABASE_URL, echo=False)

# Create a configured "Session" class
Session = sessionmaker(bind=engine)

# Create a Session
session = Session()

# ---------------------- Reflect Tables ----------------------

# Reflect the existing tables
metadata = MetaData()
try:
    metadata.reflect(bind=engine)
except SQLAlchemyError as e:
    print(f"Error reflecting database metadata: {e}")
    sys.exit(1)

# Check if required tables exist
required_tables = ['properties', 'locations']
for table_name in required_tables:
    if table_name not in metadata.tables:
        print(f"Error: Required table '{table_name}' does not exist in the database.")
        sys.exit(1)

# Define the tables
properties = metadata.tables['properties']
locations = metadata.tables['locations']

# ---------------------- Main Function ----------------------

def main():
    try:
        # ---------------------- Step 1: Fetch Unique Locations from Properties ----------------------
        print("Fetching unique locations from 'properties' table...")
        unique_locations_query = select(properties.c.location).distinct().where(properties.c.location != None).where(properties.c.location != '')
        result = session.execute(unique_locations_query)
        
        # Access the first column in each row (index 0 corresponds to 'location')
        properties_locations = [row[0].strip().lower() for row in result.fetchall()]
        
        print(f"Found {len(properties_locations)} unique locations in 'properties' table.")

        # ---------------------- Step 2: Fetch All Locations from Locations Table ----------------------
        print("Fetching existing locations from 'locations' table...")
        locations_query = select(locations.c.id, locations.c.name)
        result = session.execute(locations_query)
        
        # Create a mapping from location name to id (case-insensitive)
        # row[1] is 'name', row[0] is 'id'
        location_map = {row[1].strip().lower(): row[0] for row in result.fetchall()}
        
        print(f"Fetched {len(location_map)} locations from 'locations' table.")

        # ---------------------- Step 3: Identify Unmatched Locations ----------------------
        unmatched_locations = set()
        for loc in properties_locations:
            if loc not in location_map:
                unmatched_locations.add(loc)
        
        print(f"Found {len(unmatched_locations)} unmatched locations.")

        # ---------------------- Step 4: Update location_id for Matched Locations ----------------------
        matched_locations = set(properties_locations) - unmatched_locations
        for loc in matched_locations:
            loc_id = location_map[loc]
            # Update properties where location matches
            update_stmt = (
                update(properties).
                where(properties.c.location == loc).
                values(location_id=loc_id)
            )
            session.execute(update_stmt)
            print(f"Updated 'location_id' for properties with location '{loc}' to {loc_id}.")

        # Commit the updates for matched locations
        session.commit()
        print("All matched 'location_id' fields have been updated.")

        # ---------------------- Step 5: Handle Unmatched Locations ----------------------
        if unmatched_locations:
            print("\nThe following locations in 'properties' table did not have a matching entry in 'locations' table:")
            for loc in unmatched_locations:
                print(f"- {loc}")

            add_missing = input("\nDo you want to add these unmatched locations to the 'locations' table? (y/n): ").strip().lower()
            if add_missing == 'y':
                for loc in unmatched_locations:
                    # Insert the new location
                    insert_stmt = locations.insert().values(name=loc.title())  # Capitalize for consistency
                    result = session.execute(insert_stmt)
                    new_loc_id = result.inserted_primary_key[0]
                    print(f"Added location '{loc}' with ID {new_loc_id}.")

                    # Update properties with the new location_id
                    update_stmt = (
                        update(properties).
                        where(properties.c.location == loc).
                        values(location_id=new_loc_id)
                    )
                    session.execute(update_stmt)
                    print(f"Updated properties with location '{loc}' to 'location_id' {new_loc_id}.")

                # Commit the new inserts and updates
                session.commit()
                print("All unmatched locations have been added and updated.")
            else:
                print("Unmatched locations were not added. Please handle them manually.")
        else:
            print("All locations matched successfully.")

    except SQLAlchemyError as e:
        print(f"An error occurred: {e}")
        session.rollback()
    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
        session.rollback()
    finally:
        session.close()
        print("Database session closed.")

# ---------------------- Execute Main ----------------------

if __name__ == "__main__":
    main()
