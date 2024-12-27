import os
import shutil
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, select, update

# Directories
locationimages_old_dir = 'locationimages_old'

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

# Load mappings from CSV
input_csv = "location_image_mappings.csv"
if not os.path.exists(input_csv):
    print(f"Mapping file {input_csv} not found.")
    exit()

image_mappings_df = pd.read_csv(input_csv)

# Remove old .webp files and update database entries
for _, row in image_mappings_df.iterrows():
    webp_file = row['WebP_File']
    avif_file_name = os.path.basename(row['AVIF_File'])

    # Fetch the location name from the database
    location_name_query = select(locations_table.c.name).where(
        locations_table.c.image == webp_file.replace('locationimages_old', 'locationimages').replace("\\", "/")
    )
    location_name_result = connection.execute(location_name_query).fetchone()

    if location_name_result:
        location_name = location_name_result[0]
        avif_file = f"locationimages/{location_name}/{avif_file_name}"

        # Remove old .webp file
        if os.path.exists(webp_file):
            os.remove(webp_file)
            print(f"Deleted: {webp_file}")

        # Update database entries
        with connection.begin() as transaction:
            update_query = (
                update(locations_table)
                .where(locations_table.c.image == webp_file.replace('locationimages_old', 'locationimages').replace("\\", "/"))
                .values(image=avif_file)
            )

            result = connection.execute(update_query)

            if result.rowcount > 0:
                print(f"Updated: {webp_file} -> {avif_file}")
            else:
                print(f"No matching record found for: {webp_file}")

print("Old .webp files removed and database updated successfully.")