# store_data_db.py
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

def main():
    # psycopg2 connection for raw queries
    conn = psycopg2.connect(
        host=db_params['host'],
        database=db_params['database'],
        user=db_params['user'],
        password=db_params['password'],
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # SQLAlchemy engine for pandas
    engine = create_engine(
        f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}/{db_params["database"]}'
    )

    # Directory for cleaned CSVs
    CLEANED_DIR = "cleaned_data"
    tables = ['sessions', 'users', 'comments', 'theaters', 'movies', 'embedded_movies']

    for table in tables:
        file_path = os.path.join(CLEANED_DIR, f"{table}_cleaned.csv")
        
        # Check if table exists and has rows
        cur.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """, (table,))
        table_exists = cur.fetchone()[0]

        if table_exists:
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            row_count = cur.fetchone()[0]
        else:
            row_count = 0

        if row_count > 0:
            print(f"Skipping upload for '{table}' — already has {row_count} rows.")
        else:
            print(f"Uploading {file_path} to PostgreSQL table '{table}'...")
            df = pd.read_csv(file_path)
            df.to_sql(name=table, con=engine, if_exists='replace', index=False)
            print(f"Uploaded {table} table successfully.\n")

    print("Upload process complete.")


# Allow running standalone too
if __name__ == "__main__":
    main()
