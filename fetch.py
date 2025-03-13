#!/usr/bin/env python

#!/usr/bin/env python

import sys
import requests
import time
import csv
import concurrent.futures

# UniProt API endpoint
UNIPROT_URL = "https://rest.uniprot.org/uniprotkb/stream"

# Fields to retrieve (ALL FIELDS)
FIELDS = "accession,reviewed,id,protein_name,gene_names,organism_name,length,gene_oln,gene_orf," \
         "gene_primary,gene_synonym,organism_id,xref_proteomes,lineage,lineage_ids,virus_hosts," \
         "cc_alternative_products,ft_var_seq,error_gmodel_pred,fragment,organelle,mass,cc_mass_spectrometry," \
         "ft_variant,ft_non_cons,ft_non_std,ft_non_ter,cc_polymorphism,cc_rna_editing,sequence,cc_sequence_caution," \
         "ft_conflict,ft_unsure,sequence_version,absorption,ft_act_site,ft_binding,cc_catalytic_activity," \
         "cc_cofactor,ft_dna_bind,ec,cc_activity_regulation,cc_function,kinetics,cc_pathway,ph_dependence," \
         "temp_dependence,ft_site,rhea,redox_potential,annotation_score,cc_caution,keyword,keywordid," \
         "cc_miscellaneous,protein_existence,tools,uniparc_id,comment_count,feature_count,cc_interaction," \
         "cc_subunit,cc_developmental_stage,cc_induction,cc_tissue_specificity,go_p,go_c,go,go_f,go_id," \
         "cc_biotechnology,cc_allergen,cc_disruption_phenotype,cc_disease,ft_mutagen,cc_pharmaceutical," \
         "cc_toxic_dose,ft_intramem,cc_subcellular_location,ft_topo_dom,ft_transmem,ft_chain,ft_crosslnk," \
         "ft_disulfid,ft_carbohyd,ft_init_met,ft_mod_res,ft_lipid,ft_peptide,cc_ptm,ft_propep,ft_signal," \
         "ft_transit,structure_3d,ft_strand,ft_helix,ft_turn,lit_pubmed_id,lit_doi_id,date_created," \
         "date_modified,date_sequence_modified,version,ft_coiled,ft_compbias,cc_domain,ft_domain," \
         "ft_motif,protein_families,ft_region,ft_repeat,cc_similarity,ft_zn_fing"

# Fields that contain IDs (must be separated)
ID_FIELDS = ["ec", "go_p", "go_c", "go", "go_f", "go_id", "rhea", "keyword", "keywordid"]

BATCH_SIZE = 10  # Increased batch size for efficiency
MAX_RETRIES = 3  # Retry up to 3 times if API fails


def fetch_uniprot_data(uniprot_batch):
    """Fetches UniProt data for a batch of IDs, ensuring all IDs are returned."""
    params = {
        "query": " OR ".join(f"accession:{uid}" for uid in uniprot_batch),
        "fields": FIELDS,
        "format": "tsv"
    }

    attempt = 0
    while attempt < MAX_RETRIES:  # Retry up to MAX_RETRIES if API fails
        response = requests.get(UNIPROT_URL, params=params)

        if response.status_code == 200:
            result = response.text.strip().split("\n")
            retrieved_ids = {line.split("\t")[0] for line in result[1:]}  # Extract returned IDs

            # Check if all requested IDs are present
            missing_ids = set(uniprot_batch) - retrieved_ids
            if missing_ids:
                print(f"Warning: Missing {len(missing_ids)} IDs in batch, retrying missing IDs...")
                return None, list(missing_ids)  # Return missing IDs for retrying
            return response.text, []  # Return valid response with no missing IDs
        elif response.status_code == 429:  # Too many requests
            print("Rate limit exceeded, waiting 5 seconds before retrying...")
            time.sleep(5)
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None, uniprot_batch  # Return all IDs for retrying if an error occurs
        attempt += 1
    return None, uniprot_batch  # Return all IDs for retrying if all retries fail


def split_ids_from_descriptions(rows, header):
    """Separates ID fields from descriptions."""
    updated_rows = []
    updated_header = []

    for row in rows:
        new_row = []
        for i, value in enumerate(row):
            col_name = header[i]

            if col_name in ID_FIELDS and ";" in value:  # Split IDs
                ids = ";".join([x.split(" ")[0] for x in value.split(";")])  # Extract only IDs
                desc = ";".join([x.split(" ", 1)[1] if " " in x else x for x in value.split(";")])  # Extract descriptions
                new_row.append(ids if ids else "no information")
                new_row.append(desc if desc else "no information")

                if f"{col_name}_id" not in updated_header:  # Add new columns to the header
                    updated_header.append(f"{col_name}_id")
                    updated_header.append(f"{col_name}_desc")
            else:
                new_row.append(value if value else "no information")
                if col_name not in updated_header:
                    updated_header.append(col_name)

        updated_rows.append(new_row)

    return updated_header, updated_rows


def process_batch(uniprot_batch, writer, header_written):
    """Fetch and process a single batch of UniProt IDs."""
    result, missing_ids = fetch_uniprot_data(uniprot_batch)

    if result:
        lines = result.strip().split("\n")
        raw_header = lines[0].split("\t")
        rows = [line.split("\t") for line in lines[1:]]

        # Process and organize IDs separately
        updated_header, formatted_rows = split_ids_from_descriptions(rows, raw_header)

        # Write header only once
        if not header_written:
            writer.writerow(["Unique ID"] + updated_header[1:])  # Rename first column to 'Unique ID'
            header_written = True

        # Write formatted data
        writer.writerows(formatted_rows)

    return missing_ids, header_written


def main(input_file, output_file):
    """Reads UniProt IDs, fetches data in batches, formats, and saves to output_file."""
    with open(input_file, "r") as f:
        uniprot_ids = [line.strip() for line in f if line.strip()]

    if not uniprot_ids:
        print("Error: No UniProt IDs found in input file.")
        return

    print(f"Fetching data for {len(uniprot_ids)} UniProt IDs in batches of {BATCH_SIZE}...")

    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        header_written = False
        all_missing_ids = uniprot_ids  # Initialize missing_ids with all IDs

        while all_missing_ids:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                batches = [all_missing_ids[i:i + BATCH_SIZE] for i in range(0, len(all_missing_ids), BATCH_SIZE)]
                futures = [executor.submit(process_batch, batch, writer, header_written) for batch in batches]

                for future in concurrent.futures.as_completed(futures):
                    missing_ids, header_written = future.result()
                    all_missing_ids = missing_ids  # Update remaining missing IDs

            # Be polite to UniProt servers (optional delay)
            time.sleep(1)

    print(f"Data successfully saved to {output_file}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python fetch_uniprot_data.py <input_file> <output_file>")
    else:
        main(sys.argv[1], sys.argv[2])
