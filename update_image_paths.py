import os
from sqlalchemy import create_engine, Table, MetaData, select, update

# Database connection details
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

# Directories
uploads_dir = 'uploads'
converted_dir = 'Converted'

# Initialize database connection
metadata = MetaData()
metadata.reflect(bind=engine)
property_images_table = metadata.tables['property_images']

# Map .avif files and check against the database
with connection:
    for property_id in os.listdir(converted_dir):
        converted_property_path = os.path.join(converted_dir, property_id)
        uploads_property_path = os.path.join(uploads_dir, property_id)

        if os.path.isdir(converted_property_path) and os.path.isdir(uploads_property_path):
            for file_name in os.listdir(converted_property_path):
                if file_name.endswith('.avif'):
                    avif_file_path = os.path.join(uploads_property_path, file_name).replace("\\", "/")
                    webp_file_path = os.path.join(uploads_property_path, file_name.replace('.avif', '.webp')).replace("\\", "/")

                    # Check if .webp path exists in the database
                    query = select(property_images_table.c.image_path).where(property_images_table.c.image_path == webp_file_path)
                    result = connection.execute(query).fetchone()

                    if result:
                        try:
                            # Update .webp to .avif in the database
                            update_query = (
                                update(property_images_table)
                                .where(property_images_table.c.image_path == webp_file_path)
                                .values(image_path=avif_file_path)
                            )
                            connection.execute(update_query)
                            print(f"Updated: {webp_file_path} -> {avif_file_path}")
                        except Exception as update_error:
                            print(f"Error updating {webp_file_path} to {avif_file_path}: {update_error}")
                    else:
                        print(f"No matching record for: {webp_file_path}")

print("Database update process completed.")