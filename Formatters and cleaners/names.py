import pandas as pd
import json
import gzip

nconsts = set()
with open("title_principals.json", "r") as f:
    title_principals = json.load(f)
    for entry in title_principals:
        nconsts.add(entry["nconst"])  # Collect unique nconst values

print(f"Found {len(nconsts)} unique nconst values.")

# Filter name.basics.tsv.gz for matching nconst values
try:
    df_names = pd.read_csv("name.basics.tsv", sep="\t", dtype=str, na_values="\\N")
    df_names_filtered = df_names[df_names["nconst"].isin(nconsts)]
    df_names_filtered = df_names_filtered.fillna("")
    if not df_names_filtered.empty:
        df_names_filtered.to_json("name_basics.json", orient="records", indent=4)
        print("Saved filtered data to name_basics.json")
    else:
        print("No matching data found in name.basics.tsv.gz")

except FileNotFoundError:
    print("Error: name.basics.tsv.gz not found. Please check the filename and directory.")

print("All filtering and saving completed.")