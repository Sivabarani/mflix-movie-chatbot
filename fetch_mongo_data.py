# 1_fetch_data.py
import os
import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB Atlas
uri = "mongodb+srv://datascience:i9jgSCUgvINonxgZ@cluster0.yjrfzkw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri)

# Test connection
client.admin.command('ping')

# Load sample_mflix database
sample_mflix_db = client.get_database('sample_mflix')
collection_names = sample_mflix_db.list_collection_names()

# Folder to store raw data
RAW_DATA_DIR = "raw_data"
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# Fetch each collection and save as raw CSV
for collection_name in collection_names:
    data = list(sample_mflix_db[collection_name].find())
    if data:
        df = pd.json_normalize(data)
        file_path = os.path.join(RAW_DATA_DIR, f"{collection_name}.csv")
        df.to_csv(file_path, index=False)
        print(f"Saved {collection_name} to {file_path}")

print("All collections fetched and saved to raw_data/")
