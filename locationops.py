import os
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, select, update

# Initialize database connection
db_name = "archstone_test_db"
username = "root"
password = ""

try:
    engine = create_engine(f"mysql+pymysql://{username}:{password}@localhost/{db_name}")
    connection = engine.connect()
    print("Database connection successful.")
except Exception as e:
    print(f"Database connection failed: {e}")
    exit()

# Initialize database metadata
metadata = MetaData()
metadata.reflect(bind=engine)
locations_table = metadata.tables['locations']

# Update database entries
with connection.begin() as transaction:
    select_query = select(locations_table.c.image, locations_table.c.name)
    results = connection.execute(select_query).fetchall()

    for image_path, location_name in results:
        if location_name and image_path:
            file_name = os.path.basename(image_path)
            new_image_path = f"locationimages/{location_name}/{file_name}"

            update_query = (
                update(locations_table)
                .where(locations_table.c.image == image_path)
                .values(image=new_image_path)
            )

            result = connection.execute(update_query)

            if result.rowcount > 0:
                print(f"Updated: {image_path} -> {new_image_path}")
            else:
                print(f"No matching record found for: {image_path}")

print("Database updated successfully.")
