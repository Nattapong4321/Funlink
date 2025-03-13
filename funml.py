#!/usr/bin/env python

import os
import bz2
import shutil
import argparse
import sqlite3
import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

def decompress_db(bz2_file, output_dir):
    """Decompress the database if it's compressed."""
    db_file = os.path.join(output_dir, "efficient_data_storage.db")
    if bz2_file.endswith(".bz2"):
        print(f"📂 Decompressing {bz2_file}...")
        with bz2.BZ2File(bz2_file, "rb") as f_in, open(db_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        print(f"✅ Decompressed to {db_file}")
    return db_file

def load_data_from_sqlite(db_file):
    """Load tables from SQLite and return as a dictionary of DataFrames."""
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [table[0] for table in cursor.fetchall()]
    
    dataframes = {}
    for table in tables:
        print(f"📋 Loading table: {table}")
        df = pd.read_sql_query(f"SELECT * FROM {table};", conn)
        dataframes[table] = df
    
    conn.close()
    return dataframes

def preprocess_data(dataframes, metadata_file):
    """Merge tables from database with metadata and prepare for ML."""
    metadata = pd.read_csv(metadata_file, sep="\t")

    processed_data = []
    
    for table_name, df in dataframes.items():
        meta_row = metadata[metadata["table_name"] == table_name]
        if meta_row.empty:
            print(f"⚠️ No metadata for {table_name}. Skipping.")
            continue
        
        df["species"] = meta_row["species"].values[0]
        df["condition"] = meta_row["condition"].values[0]
        
        processed_data.append(df)
    
    return pd.concat(processed_data, ignore_index=True) if processed_data else None

def train_model(data, model_output_path):
    """Train a model with both species & condition as target variables."""
    if data is None or data.empty:
        print("❌ No data available for training.")
        return

    # Select features and targets
    X = data.drop(columns=["species", "condition"])  
    y = data[["species", "condition"]]  # Multi-output target

    # Convert categorical features
    X = pd.get_dummies(X, drop_first=True)

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train model
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Predict and evaluate
    y_pred = model.predict(X_test)
    
    acc_species = accuracy_score(y_test["species"], y_pred[:, 0])
    acc_condition = accuracy_score(y_test["condition"], y_pred[:, 1])
    
    print(f"\n🔹 Accuracy (Species): {acc_species:.4f}")
    print(f"🔹 Accuracy (Condition): {acc_condition:.4f}")

    # Save the trained model
    joblib.dump(model, model_output_path)
    print(f"✅ Model saved to {model_output_path}")

    return model

def main():
    parser = argparse.ArgumentParser(description="Load data from SQLite, update metadata, and run ML model.")
    parser.add_argument("--db_bz2_file", required=True, help="Path to compressed SQLite database")
    parser.add_argument("--output_dir", required=True, help="Output directory for decompressed DB")
    parser.add_argument("--metadata_file", required=True, help="Path to metadata TSV file")
    parser.add_argument("--model_output", required=True, help="Path to save trained ML model (.pkl)")

    args = parser.parse_args()
    
    # Decompress database
    db_file = decompress_db(args.db_bz2_file, args.output_dir)
    
    # Load data from database
    dataframes = load_data_from_sqlite(db_file)
    
    # Preprocess and train ML model
    data = preprocess_data(dataframes, args.metadata_file)
    trained_model = train_model(data, args.model_output)

if __name__ == "__main__":
    main()
