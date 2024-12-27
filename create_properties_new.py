# create_properties_new.py

import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Text, Boolean, DECIMAL, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Define the base class
Base = declarative_base()

# Define the properties_new table schema
class PropertiesNew(Base):
    __tablename__ = 'properties_new'

    id = Column(Integer, primary_key=True, autoincrement=True)
    location = Column(String(255), nullable=False)
    specific_location = Column(String(255), nullable=True)
    county_name = Column(String(100), nullable=True)
    property_type = Column(String(100), nullable=True)
    property_category = Column(String(100), nullable=True)
    description = Column(Text, nullable=True)
    bedrooms = Column(Integer, nullable=True)
    bathrooms = Column(Integer, nullable=True)
    currency = Column(String(10), nullable=True)
    price = Column(DECIMAL(15, 2), nullable=True)
    amenities = Column(Text, nullable=True)  # Consider using JSON if supported
    is_featured = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

# Database connection configuration
DB_USERNAME = 'root'        # Default XAMPP MySQL username
DB_PASSWORD = ''            # Default XAMPP MySQL password is empty
DB_HOST = 'localhost'
DB_PORT = '3306'            # Default MySQL port
DB_NAME = 'archstone_test'     # Replace with your actual database name

# Create the database URL
DATABASE_URL = f"mysql+pymysql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def main():
    # Create the SQLAlchemy engine
    try:
        engine = create_engine(DATABASE_URL, echo=True)
    except Exception as e:
        print(f"Error creating engine: {e}")
        return

    # Create all tables defined by Base's subclasses (i.e., PropertiesNew)
    try:
        Base.metadata.create_all(engine)
        print("Table 'properties_new' created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")

if __name__ == "__main__":
    main()
