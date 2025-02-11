import os
import csv
import psycopg2
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Get the connection string from the environment variable
connection_string = os.getenv("DATABASE_URL")

# File paths
RAW_FILE = "CogNetv2.tsv"
CLEANED_FILE = "cognates.csv"

# Create table if not exists
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS cognates (
    uid SERIAL PRIMARY KEY,
    concept_id VARCHAR(20) NOT NULL,
    language VARCHAR(10) NOT NULL,
    word TEXT NOT NULL,
    translit TEXT NULL
);
"""


def process_tsv(raw_file, cleaned_file):
    """Processes the TSV, removes duplicates, and saves the data in CSV format."""
    seen = set()  # To track unique rows
    with open(raw_file, "r", encoding="utf-8") as infile, open(
        cleaned_file, "w", encoding="utf-8", newline=""
    ) as outfile:
        header = next(infile)  # Skip the first line (header)

        # Write header to the output CSV
        writer = csv.writer(outfile, delimiter=",")
        writer.writerow(["concept_id", "language", "word", "translit"])

        for line in infile:
            parts = line.split("\t")  # Split by tab
            if len(parts) != 7:
                print(f"Skipping malformed row: {line}")
                continue

            concept_id, lang1, word1, lang2, word2, translit1, translit2 = parts

            # Convert empty transliterations to PostgreSQL NULL format (\N)
            translit1 = translit1.strip() if translit1.strip() else r"\N"
            translit2 = translit2.strip() if translit2.strip() else r"\N"

            # Prepare rows (one per language)
            row1 = (concept_id, lang1, word1, translit1)
            row2 = (concept_id, lang2, word2, translit2)

            # Add rows to set if they are unique (this removes duplicates)
            if row1 not in seen:
                seen.add(row1)
                writer.writerow(row1)
            if row2 not in seen:
                seen.add(row2)
                writer.writerow(row2)

    print(f"✅ Processed and de-duplicated data saved as: {cleaned_file}")


def upload_to_db(cleaned_file):
    """Uploads using COPY for fast bulk insert."""
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()

    try:
        # Ensure table exists
        cursor.execute(CREATE_TABLE_SQL)
        conn.commit()

        # Upload using COPY (for CSV format with commas as delimiter)
        with open(cleaned_file, "r", encoding="utf-8") as f:
            cursor.copy_expert(
                "COPY cognates(concept_id, language, word, translit) FROM STDIN WITH (FORMAT csv, DELIMITER ',', HEADER TRUE, NULL '\\N')",
                f,
            )
        conn.commit()
        print("🚀 Data uploaded successfully!")

    except Exception as e:
        conn.rollback()
        print("❌ Upload failed:", e)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    process_tsv(RAW_FILE, CLEANED_FILE)
    upload_to_db(CLEANED_FILE)
