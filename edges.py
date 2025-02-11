import csv
import os
import psycopg2
from dotenv import load_dotenv
from io import StringIO

# Load .env file
load_dotenv()

# Get the connection string from the environment variable
connection_string = os.getenv("DATABASE_URL")

# File paths
COGNET_FILE = "CogNetv2.tsv"
DB_COGNATES_FILE = "db_cognates.csv"
OUTPUT_CSV = "edges.csv"

# Create table if not exists
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS edges (
    uid SERIAL PRIMARY KEY,
    word1_id INTEGER NOT NULL,
    word2_id INTEGER NOT NULL,
    UNIQUE (word1_id, word2_id)
);
"""


# Step 1: Read db_cognates.csv to create a mapping of (concept_id, language, word) -> uid
def create_uid_map():
    # Create a dictionary to map (concept_id, language, word) to uid
    uid_map = {}

    with open(DB_COGNATES_FILE, mode="r", newline="", encoding="utf-8") as db_file:
        reader = csv.DictReader(db_file)

        for row in reader:
            key = (row["concept_id"], row["language"], row["word"])
            uid_map[key] = row["uid"]

    return uid_map


# Step 2: Process CogNetv2.tsv and generate edges CSV
def generate_edges_csv():

    with open(COGNET_FILE, "r", encoding="utf-8") as infile, open(
        OUTPUT_CSV, "w", encoding="utf-8", newline=""
    ) as outfile:

        uid_map = create_uid_map()

        header = next(infile)  # Skip header line in the input file
        writer = csv.writer(outfile, delimiter=",")
        writer.writerow(["word1_id", "word2_id"])

        # Set to keep track of processed edges to avoid duplicates
        processed_edges = set()

        for line in infile:
            parts = line.split("\t")
            if len(parts) != 7:
                print(f"Skipping malformed row: {line}")
                continue

            concept_id, lang1, word1, lang2, word2, translit1, translit2 = parts
            key1 = (concept_id, lang1, word1)
            key2 = (concept_id, lang2, word2)

            uid1 = uid_map.get(key1)
            uid2 = uid_map.get(key2)

            if uid1 and uid2:
                edge = tuple(
                    sorted([uid1, uid2])
                )  # Ensure consistent order for edge (no duplicates)
                if edge not in processed_edges:
                    writer.writerow([uid1, uid2])
                    processed_edges.add(edge)

        print(f"✅ Edges CSV generated successfully: {OUTPUT_CSV}")


BATCH_SIZE = 10000  # Set the batch size, adjust as needed


def upload_to_db():
    """Uploads the edges data to the database in batches."""
    with psycopg2.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            try:
                # Create the table if it does not exist
                cursor.execute(CREATE_TABLE_SQL)
                conn.commit()

                # Open the CSV file
                with open(OUTPUT_CSV, "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    header = next(reader)  # Skip the header row

                    # Process in batches
                    batch = []
                    for row in reader:
                        batch.append(row)
                        # When the batch reaches the specified size, upload it
                        if len(batch) >= BATCH_SIZE:
                            # Use StringIO to create an in-memory file-like object
                            output = StringIO()
                            writer = csv.writer(output)
                            # Write batch to the StringIO object
                            writer.writerows(batch)
                            output.seek(0)  # Rewind the StringIO object

                            # Use COPY to load the current batch
                            cursor.copy_expert(
                                "COPY edges (word1_id, word2_id) FROM STDIN WITH (FORMAT csv, DELIMITER ',', HEADER FALSE)",
                                output,
                            )
                            batch = []  # Reset batch after uploading

                    # Upload any remaining data in the batch
                    if batch:
                        output = StringIO()
                        writer = csv.writer(output)
                        writer.writerows(batch)
                        output.seek(0)

                        cursor.copy_expert(
                            "COPY edges (word1_id, word2_id) FROM STDIN WITH (FORMAT csv, DELIMITER ',', HEADER FALSE)",
                            output,
                        )

                conn.commit()
                print("✅ Edge data uploaded successfully to the database in batches.")

            except Exception as e:
                conn.rollback()
                print("❌ Error uploading edges data:", e)


# Run the function to upload the edges in batches
if __name__ == "__main__":
    # generate_edges_csv()
    upload_to_db()
