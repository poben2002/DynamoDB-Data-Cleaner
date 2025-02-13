import pandas as pd
import json

file_path = "title.basics.tsv"
df = pd.read_csv(file_path, sep="\t", dtype=str, na_values="\\N")
df = df[['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'runtimeMinutes', 'genres']]


df['isAdult'] = df['isAdult'].fillna("0")  # Ensure missing values are filled
df['isAdult'] = df['isAdult'].astype(int)  # Convert to integer (0 or 1)


df['startYear'] = pd.to_numeric(df['startYear'], errors='coerce').fillna(0).astype(int)
df['runtimeMinutes'] = pd.to_numeric(df['runtimeMinutes'], errors='coerce').fillna(0).astype(int)

#df_shuffle = df.sample(n=500)
df_shuffle = df['tconst'].sample(n=500)
#df.sample(n=200)['tconst']
movies_json = set(df_shuffle)

with open("tconsts.txt", "w") as f:
    for tcons in movies_json:
        f.write(tcons + '\n')
    #json.dump(movies_json, f, indent=4)

print("Data cleaning complete. 500 JSON objects saved for DynamoDB.")

#nconst