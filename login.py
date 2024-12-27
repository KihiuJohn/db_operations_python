import os
import re

def search_files(directory, pattern, exclude_pattern=None):
    matches = []
    for root, dirs, files in os.walk(directory):
        for filename in files:
            # Only search in PHP, Blade, and other relevant files
            if filename.endswith(('.php', '.blade.php', '.js', '.vue', '.html')):
                filepath = os.path.join(root, filename)
                print(f"Looking through: {filepath}")  # Print the filename being processed
                try:
                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as file:
                        content = file.read()
                        # Search for the pattern
                        if re.search(pattern, content):
                            # Exclude files containing the exclude_pattern
                            if exclude_pattern and re.search(exclude_pattern, content):
                                continue
                            matches.append(filepath)
                except Exception as e:
                    print(f"Error reading file {filepath}: {e}")
    return matches

def main():
    # Specify the directory to search
    directory = 'ArchAdminLaravel'

    # Pattern to search for: route named 'login' (excluding 'admin.login')
    pattern = r"route\s*\(\s*[\'\"]login[\'\"]"
    exclude_pattern = r"route\s*\(\s*[\'\"]admin\.login[\'\"]"

    print("Searching for files containing route named 'login' (excluding 'admin.login')...\n")

    matches = search_files(directory, pattern, exclude_pattern)

    if matches:
        print("\nFiles containing the route 'login' (excluding 'admin.login'):")
        for match in matches:
            print(match)
    else:
        print("\nNo files found containing the route 'login' (excluding 'admin.login').")

if __name__ == "__main__":
    main()
