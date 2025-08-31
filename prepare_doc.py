# prepare_doc.py
import psycopg2 
import pandas as pd

# Database connection parameters
db_params = {
    'host': 'localhost',
    'database': 'CHATBOT',
    'user': 'postgres',
    'password': '18shiva',
}

def run_query(cur, query):
    cur.execute(query)
    colnames = [desc[0] for desc in cur.description]
    data = cur.fetchall()
    return pd.DataFrame(data, columns=colnames)


def main():
    # Connect
    conn = psycopg2.connect(
        host=db_params['host'],
        database=db_params['database'],
        user=db_params['user'],
        password=db_params['password'],
    )
    cur = conn.cursor()

    # ---- Fetch Data ----
    df_movies = run_query(cur, """
    SELECT _id, title, plot, fullplot, genres, runtime, "cast", directors, writers,
    year, languages, released, rated, lastupdated, countries, type, num_mflix_comments, 
    "imdb.id", "imdb.rating", "imdb.votes", metacritic,
    "awards.text", "awards.wins", "awards.nominations"
    FROM movies;
    """)

    df_embedded = run_query(cur, """SELECT _id, plot_embedding FROM embedded_movies;""")

    df_theaters = run_query(cur, """
    SELECT _id, "theaterId", "location.address.street1", "location.address.city",
    "location.address.state", "location.address.zipcode", "location.geo.coordinates"
    FROM theaters;
    """)

    df_comments_users = run_query(cur, """
    SELECT c.movie_id, c.text AS review_text, u.name AS reviewer_name, u.email AS reviewer_email
    FROM comments c
    LEFT JOIN users u ON c.email = u.email;
    """)

    # ---- Format Reviews ----
    def format_review(row):
        name = row['reviewer_name'] if pd.notna(row['reviewer_name']) else "Anonymous"
        email = row['reviewer_email'] if pd.notna(row['reviewer_email']) else "NoEmail"
        text = row['review_text'] if pd.notna(row['review_text']) else ""
        return f"Reviewer: {name} ({email})\nReview: {text}"

    df_comments_users['review_full'] = df_comments_users.apply(format_review, axis=1)
    comments_grouped = df_comments_users.groupby('movie_id')['review_full'] \
                                        .apply(lambda x: '\n\n'.join(x)) \
                                        .reset_index()

    # ---- Format Theaters ----
    def format_theater(row):
        addr = f"{row['location.address.street1']}, {row['location.address.city']}, {row['location.address.state']}, {row['location.address.zipcode']}"
        geo = row['location.geo.coordinates'] if pd.notna(row['location.geo.coordinates']) else "NoGeo"
        return f"- Theater ID: {row['theaterId']}\n  Address: {addr}\n  Geo: {geo}"

    theaters_grouped = df_theaters.groupby('_id').apply(lambda g: '\n'.join(g.apply(format_theater, axis=1))).reset_index()
    theaters_grouped.rename(columns={0: 'theater_info'}, inplace=True)

    # ---- Merge all data ----
    df_movies_embedded = pd.merge(df_movies, df_embedded, on='_id', how='left')
    df_movies_embedded['is_embedded'] = df_movies_embedded['plot_embedding'].notna()

    df_temp = pd.merge(df_movies_embedded, comments_grouped,left_on='_id', right_on='movie_id', how='left')
    df_temp['review_full'] = df_temp['review_full'].fillna("No reviews available.")

    df_final = pd.merge(df_temp, theaters_grouped, on='_id', how='left')
    df_final['theater_info'] = df_final['theater_info'].fillna("No theater info available.")

    # ---- Build Document ----
    def create_document(row):
        embedded_status = "Embedded Movie" if row['is_embedded'] else "Not Embedded"
        parts = [
            f"EmbeddedStatus: {embedded_status}",
            f"Title: {row['title']}",
            f"Year: {row['year']}",
            f"Genres: {row['genres']}",
            f"Languages: {row['languages']}",
            f"Countries: {row['countries']}",
            f"Rated: {row['rated']}",
            f"Runtime: {row['runtime']}",
            f"Cast: {row['cast']}",
            f"Directors: {row['directors']}",
            f"Writers: {row['writers']}",
            f"Plot: {row['plot']}",
            f"Full Plot: {row['fullplot']}",
            f"IMDb Rating: {row['imdb.rating']} (Votes: {row['imdb.votes']})",
            f"Metacritic: {row['metacritic']}",
            f"Awards: {row['awards.text']} | Wins: {row['awards.wins']} | Nominations: {row['awards.nominations']}",
            f"Theaters:\n{row['theater_info']}",
            f"Reviews:\n{row['review_full']}"
        ]
        return '\n'.join([str(p) for p in parts if pd.notna(p) and p != ""])

    df_final['document'] = df_final.apply(create_document, axis=1)

    # ---- Save ----
    df_final_docs = df_final[['_id', 'document']].rename(columns={'_id': 'movie_id'})
    df_final_docs.to_csv('movie_full_documents_new.csv', index=False)

    print(f"Created {len(df_final_docs)} movie documents.")
    print("Sample:\n")
    print(df_final_docs['document'].iloc[0])

    # Close
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
