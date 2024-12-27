import os
import pandas as pd

# Directories
locationimages_dir = 'locationimages'
locationimages_old_dir = 'locationimages_old'

# Initialize mappings
image_mappings = []

# Scan through locationimages_old (subfolders) and locationimages (flat)
for location_name in os.listdir(locationimages_old_dir):
    old_location_path = os.path.join(locationimages_old_dir, location_name)

    if os.path.isdir(old_location_path):
        for file_name in os.listdir(old_location_path):
            if file_name.endswith('.webp'):
                webp_file_path = os.path.join(old_location_path, file_name).replace("\\", "/")
                avif_file_name = file_name.replace('.webp', '.avif')
                avif_file_path = os.path.join(locationimages_dir, avif_file_name).replace("\\", "/")

                if os.path.exists(avif_file_path):
                    image_mappings.append({
                        'WebP_File': webp_file_path,
                        'AVIF_File': avif_file_path
                    })

# Output mappings to a CSV file
output_csv = "location_image_mappings.csv"
image_mappings_df = pd.DataFrame(image_mappings)
image_mappings_df.to_csv(output_csv, index=False)

print(f"Image mappings saved to {output_csv}")