from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import select, update

# Database Configuration
db_name = 'archstone_clone_db'
username = 'root'
password = ''  # No password
db_host = 'localhost'
db_port = '3306'

# Create the database engine
engine = create_engine(f'mysql+pymysql://{username}@{db_host}:{db_port}/{db_name}')

# Connect to the database
connection = engine.connect()
metadata = MetaData()

# Reflect the property_images table
property_images = Table('property_images', metadata, autoload_with=engine)

# Select all rows matching the specified pattern
stmt = select(property_images).where(property_images.c.image_path.like('uploads/2067/%.webp'))
results = connection.execute(stmt).fetchall()

# Update image paths to .avif format
for row in results:
    old_path = str(row.image_path)  # Accessing the correct field name
    if old_path.endswith('.webp'):
        new_path = old_path.replace('.webp', '.avif')

        # Update query
        upd_stmt = (
            update(property_images)
            .where(property_images.c.image_path == old_path)
            .values(image_path=new_path)
        )
        connection.execute(upd_stmt)

# Commit the changes
connection.commit()

# Close the connection
connection.close()

print("Image paths updated successfully from .webp to .avif")
