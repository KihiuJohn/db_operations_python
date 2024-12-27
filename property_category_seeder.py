from sqlalchemy import create_engine, MetaData, Table, Column, Integer, insert, update, select, inspect, text
import pandas as pd

# Database connection parameters
DATABASE_URI = "mysql+pymysql://root:@localhost/archstone_clone"

# Connect to the database
engine = create_engine(DATABASE_URI)
connection = engine.connect()
metadata = MetaData()

# Load the tables
properties_table = Table("properties", metadata, autoload_with=engine)
property_categories_table = Table("property_categories", metadata, autoload_with=engine)

try:
    # Step 1: Check if property_category_id column exists
    inspector = inspect(engine)
    columns = [col["name"] for col in inspector.get_columns("properties")]
    if "property_category_id" not in columns:
        print("Adding property_category_id column to properties table...")
        add_column_stmt = text(
            """
            ALTER TABLE properties
            ADD COLUMN property_category_id INT DEFAULT NULL
            """
        )
        connection.execute(add_column_stmt)
    else:
        print("property_category_id column already exists, skipping addition.")

    # Step 2: Get unique property categories
    query_unique_categories = select(properties_table.c.property_category).distinct()
    result_unique_categories = connection.execute(query_unique_categories)
    unique_categories = pd.DataFrame(result_unique_categories.fetchall(), columns=["property_category"])

    # Assign unique IDs to each property category
    unique_categories["id"] = range(1, len(unique_categories) + 1)

    # Step 3: Seed the property_categories table
    print("Seeding property_categories table...")
    for _, row in unique_categories.iterrows():
        stmt_insert = insert(property_categories_table).values(
            id=row["id"],
            name=row["property_category"]
        )
        try:
            connection.execute(stmt_insert)
        except Exception as e:
            print(f"Skipping duplicate category: {row['property_category']} (Error: {e})")

    # Step 4: Update properties table with property_category_id
    print("Updating properties table...")
    for _, row in unique_categories.iterrows():
        stmt_update = (
            update(properties_table)
            .where(properties_table.c.property_category == row["property_category"])
            .values(property_category_id=row["id"])
        )
        connection.execute(stmt_update)

    # Step 5: Drop the old property_category column
    if "property_category" in columns:
        print("Dropping old property_category column...")
        drop_column_stmt = text("ALTER TABLE properties DROP COLUMN property_category")
        connection.execute(drop_column_stmt)
    else:
        print("property_category column already removed, skipping drop.")

    # Commit all changes
    connection.commit()
    print("Database successfully updated!")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Close the connection
    connection.close()
