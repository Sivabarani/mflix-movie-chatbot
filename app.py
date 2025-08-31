import os
import streamlit as st
import psycopg2
import store_data_to_db  
import clean_and_upload
import fetch_mongo_data
import prepare_doc

# ---------------------------
# Step 1: Ensure raw_data exists
# ---------------------------
RAW_DATA_DIR = "raw_data"

def ensure_raw_data():
    if not os.path.exists(RAW_DATA_DIR) or not os.listdir(RAW_DATA_DIR):
        st.warning("Raw data missing. Running fetch_mongo_data.py...")
        fetch_mongo_data.run()

# ---------------------------
# Step 2: Ensure cleaned_data exists
# ---------------------------
CLEANED_DIR = "cleaned_data"
REQUIRED_CSVS = [
    "sessions_cleaned.csv",
    "users_cleaned.csv",
    "comments_cleaned.csv",
    "theaters_cleaned.csv",
    "movies_cleaned.csv",
    "embedded_movies_cleaned.csv"
]

def ensure_cleaned_data():
    missing = [f for f in REQUIRED_CSVS if not os.path.exists(os.path.join(CLEANED_DIR, f))]
    if missing:
        st.warning(f"Missing cleaned files: {missing}. Running clean_and_upload.py...")
        clean_and_upload.run()
        
# ---------------------------
# Step 3: Ensure DB has data
# ---------------------------
def ensure_db_data():
    db_params = {
        'host': 'localhost',
        'database': 'CHATBOT',
        'user': 'postgres',
        'password': '18shiva',
    }
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    tables = ["movies", "embedded_movies", "theaters", "users", "comments", "sessions"]
    empty_tables = []
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM {t};")
        count = cur.fetchone()[0]
        if count == 0:
            empty_tables.append(t)

    if empty_tables:
        st.warning(f"Empty tables detected: {empty_tables}. Uploading data...")
        store_data_to_db.main()   # run your upload script

    cur.close()
    conn.close()

# ---------------------------
# Step 4: Ensure final document CSV
# ---------------------------
DOC_CSV = "movie_full_documents_new.csv"

def ensure_documents_csv():
    if not os.path.exists(DOC_CSV):
        st.warning("movie_full_documents_new.csv missing. Running prepare_doc.py...")
        prepare_doc.main()

# ---------------------------
# Step 5: Main Streamlit logic
# ---------------------------
def main():
    st.title("Movie Q&A Assistant (RAG + Perplexity)")

    # Ensure all prerequisites
    ensure_raw_data()
    ensure_cleaned_data()
    ensure_db_data()
    ensure_documents_csv()

    import streamlit_app_logic  
    streamlit_app_logic.main()

if __name__ == "__main__":
    main()
