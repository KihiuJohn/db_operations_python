from sqlalchemy import create_engine, MetaData, Table, update, select
import pandas as pd

# Database connection parameters
DATABASE_URI = "mysql+pymysql://root:@localhost/archstone_clone"

# Connect to the database
engine = create_engine(DATABASE_URI)
connection = engine.connect()
metadata = MetaData()

# Load the properties table
properties_table = Table("properties", metadata, autoload_with=engine)

# Mapping for corrections
correction_mapping = {
    "appartment": "apartment",  # Correct misspelling
    "Gated Community": "townhouse",
    "en-suite beautiful family home": "townhouse",
    "House": "townhouse",  # Convert "House" to "townhouse"
    "Rentals": "townhouse",
    "Rental": "townhouse",
    "House/units": "apartment",
    "House.": "apartment",
    "En-suite": "townhouse",
    "Runda": "townhouse",
    "Nyari": "townhouse",
    "Colonial looking house": "townhouse",  # Convert "Colonial looking house" to "townhouse"
    "Colonial looking house.": "townhouse",  # Handle the case with the period
    "Towwnhouse": "townhouse"  # Correct misspelling
}

try:
    # Fetch and log the unique property categories before update
    print("Unique property categories before update:")
    query_before = select(properties_table.c.property_category).distinct()
    result_before = connection.execute(query_before)
    before_categories = pd.DataFrame(result_before.fetchall(), columns=["property_category"])
    print(before_categories)

    # Update incorrect property categories in the database
    updated_rows = 0
    for old_value, new_value in correction_mapping.items():
        stmt = (
            update(properties_table)
            .where(properties_table.c.property_category == old_value)
            .values(property_category=new_value)
        )
        result = connection.execute(stmt)
        updated_rows += result.rowcount  # Count the number of rows updated

    # Handle empty or null categories by converting them to "townhouse"
    stmt_null = (
        update(properties_table)
        .where((properties_table.c.property_category.is_(None)) | (properties_table.c.property_category == ""))
        .values(property_category="townhouse")
    )
    result = connection.execute(stmt_null)
    updated_rows += result.rowcount

    # Commit changes to ensure they are saved
    connection.commit()

    print(f"Total rows updated: {updated_rows}")

    # Fetch and log the unique property categories after update
    print("Unique property categories after update:")
    query_after = select(properties_table.c.property_category).distinct()
    result_after = connection.execute(query_after)
    after_categories = pd.DataFrame(result_after.fetchall(), columns=["property_category"])
    print(after_categories)

    # Output CSV file with unique property categories
    output_csv_path = "unique_property_categories.csv"
    after_categories.to_csv(output_csv_path, index=False)
    print(f"Unique property categories have been updated and saved to: {output_csv_path}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection
    connection.close()
