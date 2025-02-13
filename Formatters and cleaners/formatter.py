import json

def convert_to_dynamodb_format(input_file, output_file):
    """ Convert a JSON file into the DynamoDB batch-write format. """

    # Load the original JSON data
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Function to format each item according to DynamoDB's expected structure
    def format_item(item):
        dynamo_item = {}
        for key, value in item.items():
            if value.isdigit():  # Convert numeric values to Number type
                dynamo_item[key] = {"N": value}
            else:
                dynamo_item[key] = {"S": value} if value else {"S": ""}
        return {"PutRequest": {"Item": dynamo_item}}

    # Convert all records -> Give table name
    dynamo_data = {"Ratings": [format_item(record) for record in data]}

    # Write to output JSON file
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dynamo_data, f, indent=4)

    print(f"Conversion completed! Output saved to {output_file}")

# Example usage
convert_to_dynamodb_format("title_ratings.json", "ftitle_ratings.json")
