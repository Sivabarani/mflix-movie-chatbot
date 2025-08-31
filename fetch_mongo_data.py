# 1_fetch_data.py
import os
import pandas as pd
from pymongo import MongoClient

RAW_DATA_DIR = "raw_data"

def run():
    """Fetch collections from MongoDB Atlas and save as CSV in raw_data/, 
    but skip if files already exist."""
    
    # Ensure raw_data folder exists
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

    # Connect to MongoDB Atlas
    uri = "mongodb+srv://datascience:i9jgSCUgvINonxgZ@cluster0.yjrfzkw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)

    # Test connection
    client.admin.command('ping')
    print("Connected to MongoDB Atlas")

    # Load sample_mflix database
    sample_mflix_db = client.get_database('sample_mflix')
    collection_names = sample_mflix_db.list_collection_names()

    for collection_name in collection_names:
        file_path = os.path.join(RAW_DATA_DIR, f"{collection_name}.csv")

        # Skip fetching if file already exists
        if os.path.exists(file_path):
            print(f"⏩ Skipping {collection_name}, already exists at {file_path}")
            continue

        # Otherwise fetch from MongoDB
        data = list(sample_mflix_db[collection_name].find())
        if data:
            df = pd.json_normalize(data)
            df.to_csv(file_path, index=False)
            print(f"Saved {collection_name} to {file_path}")

    print("New Data Collection fetch complete.")

if __name__ == "__main__":
    run()
