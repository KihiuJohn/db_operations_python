from sqlalchemy import create_engine, Column, Integer, Boolean, MetaData, Table
import pandas as pd

# Database connection details
DATABASE_NAME = 'archstone_clone'
USERNAME = 'root'
PASSWORD = ''  # Empty password
HOST = 'localhost'
TABLE_NAME = 'properties'

# Connect to the database
engine = create_engine(f'mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}/{DATABASE_NAME}')

# Define the properties table
metadata = MetaData()
properties_table = Table(
    TABLE_NAME, metadata,
    Column('id', Integer, primary_key=True),
    Column('archived', Boolean),
)

# Query the table for properties with archived = 0
with engine.connect() as connection:
    query = properties_table.select().with_only_columns(properties_table.c.id).where(properties_table.c.archived == 0)
    results = connection.execute(query)
    properties_data = pd.DataFrame(results.fetchall(), columns=['id'])

# Save the results to a CSV file
csv_filename = 'unarchived_properties.csv'
properties_data.to_csv(csv_filename, index=False)
print(f"Data successfully exported to {csv_filename}")
