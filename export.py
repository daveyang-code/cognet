import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Get the connection string from the environment variable
connection_string = os.getenv("DATABASE_URL")

# Output CSV file path
OUTPUT_CSV = "db_cognates.csv"


def export_cognates_to_csv(output_csv):
    """Export the cognates table data to a CSV file."""
    # Connect to the database
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()

    try:
        # Query to fetch all data from the cognates table
        cursor.execute("SELECT * FROM cognates")

        # Fetch all rows
        rows = cursor.fetchall()

        # Get column names from the cursor description
        columns = [desc[0] for desc in cursor.description]

        # Create a DataFrame
        df = pd.DataFrame(rows, columns=columns)

        # Write the DataFrame to a CSV file
        df.to_csv(output_csv, index=False)
        print(f"✅ Cognates table exported successfully to {output_csv}")

    except Exception as e:
        print("❌ Error exporting cognates table:", e)

    finally:
        # Close the connection
        cursor.close()
        conn.close()


if __name__ == "__main__":
    export_cognates_to_csv(OUTPUT_CSV)
