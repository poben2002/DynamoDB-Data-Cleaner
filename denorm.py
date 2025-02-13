import json
from collections import defaultdict

def load_json(filename):
    """Loads JSON file and extracts items from DynamoDB PutRequest format."""
    with open(filename, "r", encoding="utf-8") as file:
        data = json.load(file)

    # Check if data is in DynamoDB batch write format
    if isinstance(data, dict) and len(data) == 1:
        key = list(data.keys())[0]  # Extract the main key (e.g., "People", "Movies")
        extracted_data = [item["PutRequest"]["Item"] for item in data[key]]
    else:
        extracted_data = data  # Assume standard JSON format

    return extracted_data

def fix_genres_format(item):
    """Ensures 'genres' is stored as a List instead of a wrongly formatted String."""
    if "genres" in item and isinstance(item["genres"], str):
        item["genres"] = [genre.strip() for genre in item["genres"].split(",") if genre.strip()]
    return item

def extract_dynamodb_value(item):
    """Extracts the actual value from DynamoDB JSON format."""
    if isinstance(item, dict):
        key = list(item.keys())[0]  # Get "S", "N", etc.
        value = item[key]  # Extract actual value

        # Special handling for incorrectly formatted genre strings
        if key == "S" and "," in value:
            return [genre.strip() for genre in value.split(",")]  # Convert to list

        return value
    return item  # Return as-is if not in DynamoDB format

def normalize_data():
    # Load and normalize data from files
    crew_data = [ {k: extract_dynamodb_value(v) for k, v in item.items()} for item in load_json("ftitle_crew.json") ]
    episodes_data = [ {k: extract_dynamodb_value(v) for k, v in item.items()} for item in load_json("ftitle_episodes.json") ]
    movies_data = [ {k: extract_dynamodb_value(v) for k, v in item.items()} for item in load_json("ftitle_basics.json") ]
    people_data = [ {k: extract_dynamodb_value(v) for k, v in item.items()} for item in load_json("fname_basics.json") ]
    principals_data = [ {k: extract_dynamodb_value(v) for k, v in item.items()} for item in load_json("ftitle_principals.json") ]
    ratings_data = [ {k: extract_dynamodb_value(v) for k, v in item.items()} for item in load_json("ftitle_ratings.json") ]

    # Convert to dictionaries for lookup
    people_dict = {person["nconst"]: person for person in people_data}
    crew_dict = {crew["tconst"]: crew for crew in crew_data}
    ratings_dict = {rating["tconst"]: rating for rating in ratings_data}

    episodes_dict = defaultdict(list)
    for episode in episodes_data:
        episodes_dict[episode["parentTconst"]].append(episode)

    principals_dict = defaultdict(list)
    for principal in principals_data:
        principals_dict[principal["tconst"]].append(principal)

    # Normalize into DynamoDB format
    dynamodb_data = []
    for movie in movies_data:
        tconst = movie["tconst"]

        if "genres" in movie and isinstance(movie["genres"], str):
            genres_list = [genre.strip() for genre in movie["genres"].split(",") if genre.strip()]
        else:
            genres_list = movie.get("genres", [])

        # Build DynamoDB JSON structure
        dynamo_item = {
            "tconst": {"S": tconst},
            "titleType": {"S": movie.get("titleType", "")},
            "primaryTitle": {"S": movie.get("primaryTitle", "")},
            "originalTitle": {"S": movie.get("originalTitle", "")},
            "startYear": {"N": str(movie.get("startYear", 0))},
            "endYear": {"N": str(movie.get("endYear", 0))},
            "runtimeMinutes": {"N": str(movie.get("runtimeMinutes", 0))},
            "isAdult": {"N": str(movie.get("isAdult", 0))},
            "genres": {"L": [{"S": genre} for genre in genres_list]},
            "ratings": {
                "M": {
                    "averageRating": {"N": str(ratings_dict.get(tconst, {}).get("averageRating", 0))},
                    "numVotes": {"N": str(ratings_dict.get(tconst, {}).get("numVotes", 0))}
                }
            },
            "crew": {"M": crew_dict.get(tconst, {})},
            "cast": {
                "L": [
                    {
                        "M": {
                            "nconst": {"S": person["nconst"]},
                            "name": {"S": people_dict.get(person["nconst"], {}).get("primaryName", "Unknown")},
                            "category": {"S": person.get("category", "")},
                            "characters": {
                                "S": json.dumps(person.get("characters", []))  # Properly encode as JSON string
                            }
                        }
                    }
                    for person in principals_dict.get(tconst, [])
                ]
            },
            "episodes": {
                "L": [
                    {"M": episode}
                    for episode in episodes_dict.get(tconst, [])
                ]
            }
        }
       #print(f"Movie ID: {tconst} | Genres: {genres_list}")
        # Wrap in PutRequest format
        dynamodb_data.append({
            "PutRequest": {
                "Item": dynamo_item
            }
        })

    # Wrap in a table key
    final_dynamodb_json = {"Movies": dynamodb_data}

    # Save to JSON file
    with open("dynamodb_movies.json", "w", encoding="utf-8") as file:
        json.dump(final_dynamodb_json, file, indent=4)

    print("DynamoDB formatted data saved to dynamodb_movies.json")

if __name__ == "__main__":
    normalize_data()
