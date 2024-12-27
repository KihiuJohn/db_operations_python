from sqlalchemy import create_engine, MetaData, Table, update
from sqlalchemy.exc import SQLAlchemyError

def update_property_category():
    # Database connection parameters
    username = 'root'
    password = ''  # No password
    host = 'localhost'
    port = 3306  # Default MySQL port
    database = 'archstone_clone'  # <-- Replace with your actual database name

    # Create the database URL
    database_url = f'mysql+pymysql://{username}@{host}:{port}/{database}'

    # Create an engine
    engine = create_engine(database_url, echo=False)

    # Initialize metadata
    metadata = MetaData()

    try:
        # Reflect the 'properties' table from the database
        properties = Table('properties', metadata, autoload_with=engine)

        # Create the update statement
        stmt = (
            update(properties)
            .where(properties.c.property_category == 'stand alone')
            .values(property_category='stand-alone')
        )

        # Execute the update within a transaction
        with engine.connect() as connection:
            with connection.begin() as transaction:
                connection.execute(stmt)
                transaction.commit()
                print("Successfully updated 'property_category' from 'stand alone' to 'stand-alone'.")

    except SQLAlchemyError as e:
        print("An error occurred while updating the 'property_category' fields:")
        print(e)

if __name__ == "__main__":
    update_property_category()
