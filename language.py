import psycopg2
import pycountry
import pandas as pd
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Get the connection string from the environment variable
connection_string = os.getenv("DATABASE_URL")

# File path
CLEANED_FILE = "cognates.csv"

# Create language table if not exists
CREATE_LANGUAGE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS languages (
    id VARCHAR(3) PRIMARY KEY,
    language TEXT NOT NULL
);
"""


def get_language_name(iso_code):
    """Returns the full language name for an ISO 639-3 code."""
    try:
        language = pycountry.languages.get(alpha_3=iso_code)
        return language.name if language else None
    except Exception:
        return None


def upload_language(cleaned_file):
    """Process the CSV file to extract unique languages, create table, and upload data."""
    df = pd.read_csv(cleaned_file, delimiter=",", encoding="utf-8")

    # Extract unique ISO 639-3 language codes
    unique_languages = df["language"].unique()

    # Set up the database connection
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()

    try:
        cursor.execute(CREATE_LANGUAGE_TABLE_SQL)
        conn.commit()

        for iso_code in unique_languages:
            language_name = get_language_name(iso_code)
            if language_name:
                cursor.execute(
                    """
                    INSERT INTO languages (id, language) 
                    VALUES (%s, %s)
                    ON CONFLICT (id) DO NOTHING
                """,
                    (iso_code, language_name),
                )

        conn.commit()
        print("🚀 Language data uploaded successfully!")

    except Exception as e:
        conn.rollback()
        print("❌ Upload failed:", e)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    upload_language(CLEANED_FILE)
