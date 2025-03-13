#!/usr/bin/env python

import argparse
import os
import subprocess
import pandas as pd
import numpy as np
import requests
import re
import csv
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import matplotlib.pyplot as plt
import seaborn as sns
import joblib
import warnings
warnings.filterwarnings('ignore')
import time


# BLAST Pipeline Functions

def make_blast_db(protein_fasta, output_db):
    """Create a BLAST protein database."""
    cmd = [
        "makeblastdb",
        "-in", protein_fasta,
        "-dbtype", "prot",
        "-out", output_db
    ]
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

def run_blastp(db_path, query_fasta, output_file):
    """Run BLASTP search with proper parameters."""
    cmd = [
        "blastp",
        "-db", db_path,
        "-query", query_fasta,  # Corrected variable name
        "-outfmt", "6 qseqid sseqid qstart qend sstart send pident evalue ssciname staxid",
        "-out", output_file,
        "-max_target_seqs", "1",
        "-num_threads", str(os.cpu_count())
    ]
    print(f"Running: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"BLASTP failed: {str(e)}") from e

    if not os.path.exists(output_file):
        raise FileNotFoundError(f"BLAST output file not created: {output_file}")
    
    if not output_file.endswith(".tsv"):
        tsv_output_file = output_file + ".tsv"
        os.rename(output_file, tsv_output_file)
        print(f"Output renamed to: {tsv_output_file}")
        return tsv_output_file
    return output_file

def merge_files(blast_output, annotation_file, output_file, key1="qseqid", key2="TranscriptID", encoding="utf-8"):
    """Merge BLAST output with annotation file."""
    try:
        blast_cols = ["qseqid", "sseqid", "qstart", "qend", "sstart", "send",
                     "pident", "evalue", "ssciname", "staxid"]
        blast_df = pd.read_csv(blast_output, sep="\t", names=blast_cols, encoding=encoding)
        annotation_df = pd.read_csv(annotation_file, sep="\t", encoding=encoding)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Missing input file: {e.filename}") from e

    # Validate merge keys
    if key1 not in blast_df.columns:
        raise ValueError(f"Merge key '{key1}' not found in BLAST output columns: {blast_df.columns.tolist()}")
    if key2 not in annotation_df.columns:
        raise ValueError(f"Merge key '{key2}' not found in annotation file columns: {annotation_df.columns.tolist()}")

    merged_df = pd.merge(blast_df, annotation_df, left_on=key1, right_on=key2, how="left")
    merged_df.to_csv(output_file, sep="\t", index=False)
    print(f"Merged file saved to: {output_file}")
    return output_file

def extract_uniprot_ids(merged_file, uniprot_id_file):
    """Extract UniProt IDs from merged file."""
    if not os.path.exists(merged_file):
        raise FileNotFoundError(f"Merged file not found: {merged_file}")
    
    df = pd.read_csv(merged_file, sep="\t")
    
    if 'sseqid' not in df.columns:
        raise ValueError("Column 'sseqid' not found in merged file")
    
    # Extract the UniProt IDs and save to output file
    uniprot_ids = df['sseqid'].str.split('|').str[1].dropna()
    uniprot_ids.to_csv(uniprot_id_file, index=False, header=False)  # Save without index and header
    print(f"UniProt IDs saved to: {uniprot_id_file}")
    
    return uniprot_id_file


#Main Function

def main():
    parser = argparse.ArgumentParser(description="Protein Analysis Pipeline with ML", 
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    
    # BLAST pipeline arguments
    parser.add_argument("--protein_fasta", type=str, required=True,
                      help="Input protein FASTA for database creation")
    parser.add_argument("--output_db", type=str, required=True,
                      help="BLAST database output path")
    parser.add_argument("--query_fasta", type=str, required=True,
                      help="Protein FASTA file for BLASTP queries")
    parser.add_argument("--blast_output", type=str, default="blast_results.tsv",
                      help="BLAST results path")
    parser.add_argument("--annotation_file", type=str, required=True,
                      help="Annotation file path for merging")
    parser.add_argument("--merged_output", type=str, default="merged_results.tsv",
                      help="Merged output path")
    parser.add_argument("--uniprot_output", type=str, default="uniprot_data.tsv",
                      help="Final UniProt output path")
    
    
    args = parser.parse_args()

    # Run BLAST pipeline
    try:
        print("Running BLAST pipeline...")
        make_blast_db(args.protein_fasta, args.output_db)
        blast_out = run_blastp(args.output_db, args.query_fasta, args.blast_output)
        merge_files(blast_out, args.annotation_file, args.merged_output)
        id_file = extract_uniprot_ids(args.merged_output, args.uniprot_output)
        print("\nBLAST pipeline completed successfully!")
    except Exception as e:
        print(f"\nPipeline failed: {str(e)}")
        return


if __name__ == "__main__":
    main()
