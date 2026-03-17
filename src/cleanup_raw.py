import os
import glob

def cleanup_raw_data():
    # Define the folders to clean
    target_folders = [
        os.path.join('..', 'data', 'raw', 'bitcoin'),
        os.path.join('..', 'data', 'raw', 'ethereum')
    ]

    # Extensions we want to remove (the heavy stuff)
    extensions_to_delete = ['*.csv', '*.zip']

    print("--- Starting Cleanup of Raw Data ---")
    
    files_deleted = 0

    for folder in target_folders:
        if not os.path.exists(folder):
            print(f"⚠️ Folder not found: {folder}. Skipping.")
            continue

        for extension in extensions_to_delete:
            # Construct the search pattern (e.g., ../data/raw/bitcoin/*.csv)
            pattern = os.path.join(folder, extension)
            files = glob.glob(pattern)

            for file_path in files:
                try:
                    os.remove(file_path)
                    print(f"🗑️ Deleted: {os.path.basename(file_path)}")
                    files_deleted += 1
                except Exception as e:
                    print(f"❌ Error deleting {file_path}: {e}")

    if files_deleted == 0:
        print("✨ No raw files found to delete. Everything is already clean!")
    else:
        print(f"✅ Cleanup finished. Total files removed: {files_deleted}")

if __name__ == "__main__":
    cleanup_raw_data()