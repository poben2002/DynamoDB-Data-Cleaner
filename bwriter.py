import json
import boto3
import time

# AWS Configuration (Update these!)
TABLE_NAME = "Movies"  # Your actual DynamoDB table name
AWS_REGION = "us-west-2"  # Change if using a different AWS region

# Initialize DynamoDB client
dynamodb = boto3.client("dynamodb", region_name=AWS_REGION)

# Load JSON file (Updated to fname_basics.json)
with open("dynamodb_movies.json", "r", encoding="utf-8") as f:
    data = json.load(f)["Movies"]  # Extract the list inside movies

# Function to remove duplicate partition keys (if necessary)
def remove_duplicates(data):
    """Removes items with duplicate partition keys ('tconst') from the list."""
    seen = set()
    unique_items = []
    
    for item in data:
        partition_key = item['PutRequest']['Item']['tconst']['S']
        
        # If the partition key has not been seen before, add it
        if partition_key not in seen:
            seen.add(partition_key)
            unique_items.append(item)
    
    return unique_items

# Split data into chunks of 25
def chunk_data(items, chunk_size=25):
    """Splits a list into smaller chunks of a given size."""
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]

def clean_item(item):
    """Removes empty string values from numeric fields and ensures titles are strings."""
    keys_to_remove = []  # Collect keys to remove

    # Ensure 'primaryTitle' and 'originalTitle' are strings, not lists
    if isinstance(item.get("primaryTitle", {}).get("S"), list):
        item["primaryTitle"]["S"] = ', '.join(item["primaryTitle"]["S"])  # Join list into a single string
    if isinstance(item.get("originalTitle", {}).get("S"), list):
        item["originalTitle"]["S"] = ', '.join(item["originalTitle"]["S"])  # Join list into a single string

    # Check for empty string in numeric fields
    for key, value in item.items():
        if isinstance(value, dict) and "N" in value and value["N"] == "":
            keys_to_remove.append(key)  # Mark the key for removal

    for key in keys_to_remove:
        del item[key]  # Remove the keys after iteration

    return item


def fix_crew_format(item):
    """Ensures 'crew' attributes use valid DynamoDB format."""
    if "crew" in item and "M" in item["crew"]:
        crew_data = item["crew"]["M"]

        # Remove 'tconst' from crew, since it shouldn't be inside the crew map
        crew_data.pop("tconst", None)

        # Fix "directors" field: Convert list to DynamoDB list format
        if "directors" in crew_data and isinstance(crew_data["directors"], list):
            crew_data["directors"] = {"L": [{"S": d} for d in crew_data["directors"]]}
        else:
            crew_data["directors"] = {"L": []}  # Ensure it's always a list

        # Fix "writers" field: Convert list to DynamoDB list format
        if "writers" in crew_data and isinstance(crew_data["writers"], list):
            crew_data["writers"] = {"L": [{"S": w} for w in crew_data["writers"]]}
        else:
            crew_data["writers"] = {"L": []}  # Ensure it's always a list

        item["crew"]["M"] = crew_data  # Update the crew field
    
    return item



def batch_write(items):
    """Writes items in batches to DynamoDB and handles unprocessed items."""
    for batch in chunk_data(items, 25):  # AWS batch limit is 25
        for request in batch:
            item = request["PutRequest"]["Item"]
            item = clean_item(item)
            item = fix_crew_format(item)
            request["PutRequest"]["Item"] = item

        request_items = {TABLE_NAME: batch}
        response = dynamodb.batch_write_item(RequestItems=request_items)  # Define response!

        # Retry unprocessed items
        while response.get("UnprocessedItems"):
            print("Retrying unprocessed items...")
            time.sleep(1)  # Short delay to avoid throttling
            response = dynamodb.batch_write_item(RequestItems=response["UnprocessedItems"])

# Remove duplicates from the data
data_no_duplicates = remove_duplicates(data)

# Run batch write on the filtered data
batch_write(data_no_duplicates)

print("✅ Data upload completed successfully!")
