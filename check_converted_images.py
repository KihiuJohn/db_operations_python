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

# Initialize database connection
metadata = MetaData()
metadata.reflect(bind=engine)
property_images_table = metadata.tables['property_images']

# Directory for uploads
uploads_dir = 'uploads'

# Map .avif files in uploads to their .webp counterparts in the database
with connection.begin() as transaction:
    for property_id in os.listdir(uploads_dir):
        property_path = os.path.join(uploads_dir, property_id)

        if os.path.isdir(property_path):
            for file_name in os.listdir(property_path):
                if file_name.endswith('.avif'):
                    avif_file_path = os.path.join(property_path, file_name).replace("\\", "/")
                    webp_file_name = file_name.replace('.avif', '.webp')
                    webp_file_path = os.path.join(property_path, webp_file_name).replace("\\", "/")

                    # Check if the .webp path exists in the database
                    query = select(property_images_table.c.image_path).where(property_images_table.c.image_path == webp_file_path)
                    result = connection.execute(query).fetchone()

                    if result:
                        try:
                            # Update the .webp path to .avif in the database
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
