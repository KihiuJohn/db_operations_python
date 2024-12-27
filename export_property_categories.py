from sqlalchemy import create_engine, MetaData, Table, select
import pandas as pd

# Database connection parameters
DATABASE_URI = "mysql+pymysql://root:@localhost/archstone_clone"

# Connect to the database
engine = create_engine(DATABASE_URI)
connection = engine.connect()
metadata = MetaData()

# Load the property_categories table
property_categories_table = Table("property_categories", metadata, autoload_with=engine)

try:
    # Query all data from the property_categories table
    query = select(property_categories_table)
    result = connection.execute(query)

    # Fetch data into a DataFrame
    df_property_categories = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Output CSV file
    output_csv_path = "property_categories.csv"
    df_property_categories.to_csv(output_csv_path, index=False)
    print(f"Data exported successfully to {output_csv_path}")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Close the connection
    connection.close()
