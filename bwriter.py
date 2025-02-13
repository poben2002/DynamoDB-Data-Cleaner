import json
import boto3
import time

# AWS Configuration (Update these!)
TABLE_NAME = "People"  # Your actual DynamoDB table name
AWS_REGION = "us-west-2"  # Change if using a different AWS region

# Initialize DynamoDB client
dynamodb = boto3.client("dynamodb", region_name=AWS_REGION)

# Load JSON file (Updated to fname_basics.json)
with open("fname_basics.json", "r", encoding="utf-8") as f:
    data = json.load(f)["People"]  # Extract the list inside "Principals"

# Function to remove duplicate partition keys (if necessary)
def remove_duplicates(data):
    """Removes items with duplicate partition keys ('tconst') from the list."""
    seen = set()
    unique_items = []
    
    for item in data:
        partition_key = item['PutRequest']['Item']['nconst']['S']
        
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

# Batch write function
def batch_write(items):
    """Writes items in batches to DynamoDB and handles unprocessed items."""
    for batch in chunk_data(items, 25):  # AWS batch limit is 25
        request_items = {TABLE_NAME: batch}
        response = dynamodb.batch_write_item(RequestItems=request_items)

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
