#!/usr/bin/env python

#!/usr/bin/env python

import pandas as pd
import argparse

# Set up command-line argument parsing
parser = argparse.ArgumentParser(description="Merge two TSV files on specified columns.")
parser.add_argument("merge_results", help="Path to the merge_results.tsv file.")
parser.add_argument("uniprot_map", help="Path to the uniprot_map.tsv file.")
parser.add_argument("output", help="Path to save the merged output file.")

# Parse the arguments
args = parser.parse_args()

# Load merge_results
merge_results = pd.read_csv(args.merge_results, sep='\t')

# Ensure 'sseqid' exists before splitting
if 'sseqid' in merge_results.columns:
    split_sseqid = merge_results['sseqid'].str.split('|', expand=True).fillna('')
    merge_results['sseqid_1'] = split_sseqid[0]
    merge_results['sseqid_2'] = split_sseqid[1]
    merge_results['sseqid_3'] = split_sseqid[2]
else:
    print("Error: Column 'sseqid' not found in merge_results.tsv")
    exit(1)

# Overwrite the original merge_results file with the updated version
merge_results.to_csv(args.merge_results, sep='\t', index=False)

# Clean uniprot_map: Remove duplicate header rows (except the first)
with open(args.uniprot_map, 'r') as f:
    lines = f.readlines()

# Identify the first header line
header = lines[0].strip().split('\t')

# Remove rows that exactly match the header (except the first)
cleaned_lines = [lines[0]] + [line for line in lines[1:] if line.strip().split('\t') != header]

# Write back the cleaned file
with open(args.uniprot_map, 'w') as f:
    f.writelines(cleaned_lines)

# Load cleaned uniprot_map into a DataFrame
uniprot_map = pd.read_csv(args.uniprot_map, sep='\t')

# Strip spaces and force lowercase for better matching
merge_results['sseqid_3'] = merge_results['sseqid_3'].str.strip().str.lower()
uniprot_map['Entry Name'] = uniprot_map['Entry Name'].str.strip().str.lower()

# Merge using LEFT JOIN to keep all rows from merge_results
merged_df = pd.merge(merge_results, uniprot_map, left_on='sseqid_3', right_on='Entry Name', how='left')

# Sort rows based on the first column
merged_df = merged_df.sort_values(by=merged_df.columns[0])

# List of columns to remove
columns_to_remove = ['ssciname', 'staxid', 'Name', 'Alias/Synonyms', 'EC_number', 'BUSCO', 
                     'InterPro', 'EggNog', 'COG', 'GO Terms', 'Secreted', 'Membrane', 
                     'Protease', 'CAZyme', 'Notes']

# Remove specified columns if they exist
merged_df = merged_df.drop(columns=[col for col in columns_to_remove if col in merged_df.columns])

# Replace empty values with 'no information'
merged_df = merged_df.fillna('no information')

# Drop duplicate rows based on the first column (Unique ID)
merged_df = merged_df.drop_duplicates(subset=merged_df.columns[0], keep='first')

# Save final merged file
merged_df.to_csv(args.output, sep='\t', index=False)

print(f"Duplicate header rows removed from uniprot_map.tsv! Unique lines based on Unique ID merged correctly. Output saved to {args.output}")
