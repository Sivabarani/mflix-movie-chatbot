from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import os

# PostgreSQL connection parameters
db_params = {
    'host': 'localhost',
    'database': 'CHATBOT',
    'user': 'postgres',
    'password': '18shiva',
}

# Connect with psycopg2 (optional for raw queries)
conn = psycopg2.connect(
    host=db_params['host'],
    database=db_params['database'],
    user=db_params['user'],
    password=db_params['password'],
)
conn.set_session(autocommit=True)
cur = conn.cursor()

# SQLAlchemy engine for pandas to_sql upload
engine = create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}/{db_params["database"]}')

# Directory where cleaned CSVs are saved
CLEANED_DIR = "cleaned_data"

if __name__ == "__main__":
    tables = ['sessions', 'users', 'comments', 'theaters', 'movies', 'embedded_movies']

    for table in tables:
        file_path = os.path.join(CLEANED_DIR, f"{table}_cleaned.csv")
        print(f"Uploading {file_path} to PostgreSQL table '{table}'...")

        # Read the cleaned CSV file
        df = pd.read_csv(file_path)

        # Upload to PostgreSQL - replace table if exists
        df.to_sql(name=table, con=engine, if_exists='replace', index=False)

        print(f"Uploaded {table} table successfully.\n")

    print("All cleaned tables uploaded to PostgreSQL.")
