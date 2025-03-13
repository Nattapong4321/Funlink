#!/usr/bin/env python

import sqlite3
import pandas as pd
import zlib
import bz2
import os
import argparse

def decompress_database(db_path):
    """
    Decompresses a .bz2 database if it exists.
    """
    bz2_path = db_path + ".bz2"
    if os.path.exists(bz2_path):
        print(f"📂 Found compressed database. Decompressing {bz2_path}...")
        with bz2.BZ2File(bz2_path, 'rb') as bz2_file, open(db_path, 'wb') as db_file:
            db_file.write(bz2_file.read())
        print(f"✅ Decompressed database to {db_path}")

def decompress_column(df):
    """
    Decompress all columns that were compressed (i.e., of type 'object' and compressed).
    """
    for col in df.columns:
        if df[col].dtype == 'object':  # Check if the column is of type 'object' (string in pandas)
            print(f"💾 Decompressing column: {col}")
            df[col] = df[col].apply(lambda x: zlib.decompress(x).decode('utf-8') if isinstance(x, bytes) else x)
    return df

def retrieve_data_from_db(db_file, species, condition, isolate):
    """
    Retrieves data from the SQLite database for the given species, condition, and isolate.
    """
    # Decompress the database if it's compressed
    decompress_database(db_file)

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Construct the table name based on the given keys
    table_name = f"{species}_{condition}_{isolate}"

    # Query to retrieve the data
    query = f"SELECT * FROM '{table_name}'"
    df = pd.read_sql(query, conn)

    # Decompress any columns that were compressed
    df = decompress_column(df)

    # Close the database connection
    conn.close()

    return df

def save_data_to_tsv(df, output_dir, species, condition, isolate):
    """
    Saves the retrieved data as a .tsv file.
    """
    if df is not None:
        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Generate the full file path
        output_file = os.path.join(output_dir, f"{species}_{condition}_{isolate}_retrieved_data.tsv")
        
        # Save the DataFrame to a TSV file
        df.to_csv(output_file, sep="\t", index=False)
        print(f"📁 Retrieved data saved to {output_file}")
    else:
        print("❌ No data to save.")

# Command-line argument parsing
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retrieve data from SQLite database and save as TSV.")
    parser.add_argument("--species", required=True, help="Species name for retrieving data")
    parser.add_argument("--condition", required=True, help="Condition for retrieving data")
    parser.add_argument("--isolate", required=True, help="Isolate for retrieving data")
    parser.add_argument("--db_file", required=True, help="Path to the SQLite database file")
    parser.add_argument("--output_dir", required=True, help="Directory to save the retrieved data")

    args = parser.parse_args()

    # Retrieve the data from the database
    df = retrieve_data_from_db(args.db_file, args.species, args.condition, args.isolate)

    # Save the data as a .tsv file
    save_data_to_tsv(df, args.output_dir, args.species, args.condition, args.isolate)
