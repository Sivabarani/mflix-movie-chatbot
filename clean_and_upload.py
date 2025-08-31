import os
import pandas as pd

RAW_DIR = "raw_data"
CLEANED_DIR = "cleaned_data"
os.makedirs(CLEANED_DIR, exist_ok=True)

EXPECTED_FILES = [
    "sessions_cleaned.csv",
    "users_cleaned.csv",
    "comments_cleaned.csv",
    "theaters_cleaned.csv",
    "movies_cleaned.csv",
    "embedded_movies_cleaned.csv"
]

class BaseCleaner:
    def __init__(self, df):
        self.df = df

    def clean(self):
        # default no cleaning
        return self.df

class SessionsCleaner(BaseCleaner):
    # No nulls - save as is
    pass

class UsersCleaner(BaseCleaner):
    # No nulls - save as is
    pass

class CommentsCleaner(BaseCleaner):
    # No nulls - save as is
    pass

class TheatersCleaner(BaseCleaner):
    def clean(self):
        # Drop 'location.address.street2'
        col_to_drop = "location.address.street2"
        if col_to_drop in self.df.columns:
            self.df.drop(columns=[col_to_drop], inplace=True)
        return self.df

class MoviesCleaner(BaseCleaner):
    def clean(self):
        # Drop tomatoes columns
        tomatoes_cols = [col for col in self.df.columns if col.startswith("tomatoes.")]
        self.df.drop(columns=tomatoes_cols, inplace=True)
        
        # Fill nulls:
        # Text columns
        text_cols = [
            "plot", "genres", "cast", "fullplot", "languages",
            "directors", "rated", "countries", "type",
            "awards.text", "writers", "poster"  # poster added here
        ]
        for col in text_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].fillna("")
        
        # Numeric columns
        num_cols = [
            "runtime", "awards.wins", "awards.nominations",
            "imdb.rating", "imdb.votes", "num_mflix_comments",
            "metacritic"   # metacritic added here
        ]
        for col in num_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
        
        # Date columns
        date_cols = ["released", "lastupdated"]
        for col in date_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].fillna("unknown")
        
        return self.df

class EmbeddedMoviesCleaner(BaseCleaner):
    def clean(self):
        # Drop tomatoes columns
        tomatoes_cols = [col for col in self.df.columns if col.startswith("tomatoes.")]
        self.df.drop(columns=tomatoes_cols, inplace=True)
        
        # Fill nulls:
        # Text columns
        text_cols = [
            "plot", "genres", "cast", "fullplot", "languages",
            "directors", "rated", "countries", "type",
            "awards.text", "writers", "poster"  # poster added here
        ]
        for col in text_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].fillna("")
        
        # Numeric columns (including plot_embedding)
        num_cols = [
            "runtime", "awards.wins", "awards.nominations",
            "imdb.rating", "imdb.votes", "num_mflix_comments",
            "plot_embedding", "metacritic"  # metacritic added here
        ]
        for col in num_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
        
        # Date columns
        date_cols = ["released", "lastupdated"]
        for col in date_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].fillna("unknown")
        
        return self.df

def check_nulls_duplicates(df, table_name):
    print(f"Checking {table_name} for nulls and duplicates:")
    nulls = df.isna().sum()
    print("Null values per column:")
    print(nulls[nulls > 0] if not nulls[nulls > 0].empty else "No nulls found")
    duplicates = df.duplicated().sum()
    print(f"Number of duplicate rows: {duplicates}\n")

def clean_and_save(table_name):
    print(f"Cleaning {table_name} ...")
    df = pd.read_csv(f"{RAW_DIR}/{table_name}.csv")

    cleaners = {
        "sessions": SessionsCleaner,
        "users": UsersCleaner,
        "comments": CommentsCleaner,
        "theaters": TheatersCleaner,
        "movies": MoviesCleaner,
        "embedded_movies": EmbeddedMoviesCleaner
    }

    cleaner_class = cleaners.get(table_name, BaseCleaner)
    cleaner = cleaner_class(df)
    cleaned_df = cleaner.clean()
    
    # Check nulls and duplicates after cleaning
    check_nulls_duplicates(cleaned_df, table_name)

    cleaned_df.to_csv(f"{CLEANED_DIR}/{table_name}_cleaned.csv", index=False)
    print(f"Saved cleaned {table_name} to {CLEANED_DIR}/{table_name}_cleaned.csv\n")

def run():
    """Run cleaning only if ALL expected cleaned files are missing."""
    missing = [f for f in EXPECTED_FILES if not os.path.exists(os.path.join(CLEANED_DIR, f))]

    if not missing:
        print("Skipping cleaning, all cleaned CSVs already exist.")
        return

    print(f"Missing {len(missing)} cleaned files: {missing}")
    print("Running cleaning process...")

    tables = ["sessions","users","comments","theaters","movies","embedded_movies"]
    for table in tables:
        clean_and_save(table)

    print("Cleaning complete.")
    
if __name__ == "__main__":
    run()
    print("All tables cleaned and saved.")
