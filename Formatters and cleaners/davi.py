import pandas as pd
import json

# Load the tconsts from file
tconst_file = "tconsts.txt"
with open(tconst_file, "r") as f:
    tconsts = set(line.strip() for line in f)

# Function to filter TSV data
def filter_tsv(file_path, filter_column, output_file):
    df = pd.read_csv(file_path, sep="\t", dtype=str, na_values="\\N")
    df_filtered = df[df[filter_column].isin(tconsts)]
    df_filtered = df_filtered.fillna("")
    if not df_filtered.empty:
        df_filtered.to_json(output_file, orient="records", indent=4)
        print(f"Saved filtered data to {output_file}")
    else:
        print(f"No matching data found for {file_path}")

# Process each TSV file and save separate JSON files
filter_tsv("title.basics.tsv", "tconst", "title_basics.json")
filter_tsv("title.crew.tsv", "tconst", "title_crew.json")
filter_tsv("title.episode.tsv", "tconst", "title_episodes.json")
filter_tsv("title.principals.tsv", "tconst", "title_principals.json")
filter_tsv("title.ratings.tsv", "tconst", "title_ratings.json")

print("All filtering and saving completed.")
