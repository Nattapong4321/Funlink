#!/usr/bin/env python

import argparse
import os
import subprocess
import pandas as pd
import requests
import csv
import re

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
    """Run BLASTP search."""
    cmd = [
        "blastp",
        "-db", db_path,
        "-query", query_fasta,
        "-outfmt", "6 qseqid sseqid qstart qend sstart send pident evalue ssciname staxid",
        "-out", output_file,
        "-max_target_seqs", "1"
    ]
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

    # Add .tsv extension if not already present
    if not output_file.endswith(".tsv"):
        tsv_output_file = output_file + ".tsv"
        os.rename(output_file, tsv_output_file)
        print(f"Output renamed to: {tsv_output_file}")

def merge_files(blast_output, annotation_file, output_file, key1, key2, encoding="utf-8"):
    """Merge BLAST output with an annotation file based on specified keys."""
    print(f"Reading BLAST output from: {blast_output}")
    blast_df = pd.read_csv(blast_output, sep="\t", encoding=encoding)
    print(f"Reading annotation file from: {annotation_file}")
    annotation_df = pd.read_csv(annotation_file, sep="\t", encoding=encoding)

    print(f"Merging files on keys: {key1} (BLAST) and {key2} (Annotation)")
    merged_df = pd.merge(blast_df, annotation_df, left_on=key1, right_on=key2)

    print(f"Saving merged output to: {output_file}")
    merged_df.to_csv(output_file, sep="\t", index=False)
    print(f"Merged file saved successfully.")

def extract_uniprot_ids(merged_file, uniprot_id_file):
    """Extract UniProt IDs from the merged file."""
    print(f"Extracting UniProt IDs from: {merged_file}")
    cmd = f"awk -F'\t' '{{print $46}}' {merged_file} > {uniprot_id_file}"
    subprocess.run(cmd, shell=True, check=True)

    no_header_file = uniprot_id_file.replace(".txt", "_no_header.txt")
    cmd = f"grep -v 'Uniprot_ID' {uniprot_id_file} > {no_header_file}"
    subprocess.run(cmd, shell=True, check=True)

    final_uniprot_file = no_header_file.replace("_no_header.txt", "_final.txt")
    cmd = f"awk -F'|' '{{print $2}}' {no_header_file} > {final_uniprot_file}"
    subprocess.run(cmd, shell=True, check=True)

    print(f"Final UniProt IDs saved to: {final_uniprot_file}")
    return final_uniprot_file

def fetch_uniprot_data(uniprot_id):
    url = f"https://rest.uniprot.org/uniprotkb/{uniprot_id}.txt"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Failed to retrieve data for {uniprot_id}: {response.status_code}")
        return None

    return response.text

def parse_uniprot_data(data):
    # Regex patterns for each field
    patterns = {
        "Reviewed": r"^ID\s+\S+\s+(\w+);",
        "Entry Name": r"^ID\s+(\S+)",
        "Protein names": r"^DE\s+RecName: Full=(.*?);",
        "Gene Names": r"^GN\s+Name=(.*?);",
        "Organism": r"^OS\s+(.+)",
        "Length": r"^SQ\s+SEQUENCE\s+(\d+)\s+AA;",
        "Pathway": r"^CC\s+-!- PATHWAY:\s(.+?)\.",
        "Function [CC]": r"^CC\s+-!- FUNCTION:\s(.+?)\.",
        "Gene Ontology (biological process)": r"^DR\s+GO;\sGO:[0-9]+;\sP:(.*?);",
        "Gene Ontology (cellular component)": r"^DR\s+GO;\sGO:[0-9]+;\sC:(.*?);",
        "Gene Ontology (molecular function)": r"^DR\s+GO;\sGO:[0-9]+;\sF:(.*?);",
        "Intramembrane": r"^FT\s+INTRAMEM\s+(\d+.+)",
        "Subcellular location [CC]": r"^CC\s+-!- SUBCELLULAR LOCATION:\s(.+?)\.",
        "Transmembrane": r"^FT\s+TRANSMEM\s+(\d+.+)",
        "Topological domain": r"^FT\s+TOPO_DOM\s+(\d+.+)",
        "Involvement in disease": r"^CC\s+-!- DISEASE:\s(.+?)\.",
    }

    result = {}
    for field, pattern in patterns.items():
        match = re.search(pattern, data, re.MULTILINE)
        result[field] = match.group(1) if match else "Not available"

    return result

def save_to_tsv(data_list, output_file):
    with open(output_file, "w", newline="") as tsvfile:
        writer = csv.writer(tsvfile, delimiter="\t")
        # Write header
        if data_list:
            writer.writerow(data_list[0].keys())  # Header row
        # Write rows
        for data in data_list:
            writer.writerow(data.values())

def process_uniprot_ids(input_file, output_file):
    with open(input_file, "r") as infile:
        uniprot_ids = infile.read().splitlines()

    data_list = []
    for uniprot_id in uniprot_ids:
        print(f"Processing {uniprot_id}...")
        raw_data = fetch_uniprot_data(uniprot_id)
        if raw_data:
            parsed_data = parse_uniprot_data(raw_data)
            data_list.append(parsed_data)

    save_to_tsv(data_list, output_file)
    print(f"Data saved to {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description="Create a BLAST protein database, run BLASTP search, merge results with annotation, and fetch UniProt information."
    )
    parser.add_argument("--protein_fasta", required=True, help="Path to the input protein FASTA file for BLAST database creation.")
    parser.add_argument("--output_db", required=True, help="Output path for the BLAST database.")
    parser.add_argument("--query_fasta", required=True, help="Path to the query protein FASTA file.")
    parser.add_argument("--blast_output", required=True, help="Output path for BLASTP results.")
    parser.add_argument("--annotation_file", required=True, help="Path to the annotation file for merging.")
    parser.add_argument("--merged_output", required=True, help="Output path for the merged file.")
    parser.add_argument("--key1", required=True, help="Column name from BLAST output to use as the merge key.")
    parser.add_argument("--key2", required=True, help="Column name from annotation file to use as the merge key.")
    parser.add_argument("--encoding", default="utf-8", help="Encoding of the input files (default: utf-8).")
    parser.add_argument("--uniprot_output", required=True, help="Output path for UniProt information.")

    args = parser.parse_args()

    # Step 1: Create a BLAST database
    print("Step 1: Creating BLAST database...")
    make_blast_db(args.protein_fasta, args.output_db)

    # Step 2: Run BLASTP search
    print("Step 2: Running BLASTP search...")
    run_blastp(args.output_db, args.query_fasta, args.blast_output)

    # Step 3: Merge BLAST output with annotation
    print("Step 3: Merging BLAST output with annotation file...")
    merge_files(
        blast_output=args.blast_output,
        annotation_file=args.annotation_file,
        output_file=args.merged_output,
        key1=args.key1,
        key2=args.key2,
        encoding=args.encoding
    )

    # Step 4: Extract UniProt IDs from the merged file
    print("Step 4: Extracting UniProt IDs from the merged file...")
    uniprot_id_file = args.uniprot_output.replace(".tsv", "_ids.txt")
    final_uniprot_file = extract_uniprot_ids(args.merged_output, uniprot_id_file)

    # Step 5: Fetch and process UniProt data
    print("Step 5: Fetching and processing UniProt data...")
    process_uniprot_ids(final_uniprot_file, args.uniprot_output)

    print("Pipeline completed successfully!")

if __name__ == "__main__":
    main()
