from sqlalchemy import create_engine, MetaData, Table, select
import pandas as pd

# Database connection parameters
DATABASE_URI = "mysql+pymysql://root:@localhost/archstone_clone"

# Connect to the database
engine = create_engine(DATABASE_URI)
connection = engine.connect()
metadata = MetaData()

# Load the properties table
properties_table = Table("properties", metadata, autoload_with=engine)

# Query to get all unique property categories and their corresponding property IDs
query = select(
    properties_table.c.id.label("id"),  # Use `id` as the primary key
    properties_table.c.property_category.label("property_category")
)

# Execute the query and fetch data into a DataFrame
result = connection.execute(query)
df_properties = pd.DataFrame(result.fetchall(), columns=["id", "property_category"])

# Assign unique IDs to each property category
unique_categories = df_properties["property_category"].drop_duplicates().reset_index(drop=True)
category_mapping = {category: idx + 1 for idx, category in enumerate(unique_categories)}
df_properties["property_category_id"] = df_properties["property_category"].map(category_mapping)

# Output CSV file with id and property_category_id
output_csv_path = "property_id_category_mapping.csv"
df_properties[["id", "property_category", "property_category_id"]].to_csv(output_csv_path, index=False)

# Close the database connection
connection.close()

print(f"CSV file generated at: {output_csv_path}")
