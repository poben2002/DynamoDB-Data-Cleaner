# DynamoDB-Data-Cleaner
Movie Data Processing for AWS DynamoDB
This repository provides a set of utilities for cleaning, denormalizing, and batch-writing movie data from TSV files into AWS DynamoDB. The data pipeline involves removing duplicates, cleaning fields, ensuring proper data formatting, and performing batch writes to DynamoDB.

Features
Read TSV files: The pipeline reads raw TSV (Tab-Separated Values) files containing movie data.
Clean data: Removes duplicates based on partition keys and cleans fields with invalid values.
Denormalize data: Ensures nested fields such as crew and other lists are formatted correctly for DynamoDB.
Batch write to DynamoDB: Writes data in batches to DynamoDB for efficient data insertion.
Prerequisites
Before using this repository, ensure you have the following prerequisites:

AWS Account: You must have an AWS account with appropriate permissions to write to DynamoDB.
Python 3.6+: This repository is built with Python 3 and requires the following libraries:
boto3 (AWS SDK for Python)
json
You can install the necessary dependencies with pip:

bash
Copy
Edit
pip install boto3
Setup
1. AWS Configuration
Make sure you have your AWS credentials configured properly. You can set up your AWS credentials in one of the following ways:

AWS CLI: Run aws configure to set your AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and default region.
Environment variables: Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_DEFAULT_REGION in your environment.
2. Prepare Your TSV Data
Ensure your data file is in TSV (Tab-Separated Values) format. The script expects the TSV file to be structured with movie data, including columns like tconst, primaryTitle, originalTitle, and crew.

3. Update Configuration
In the bwriter.py script, update the following configuration settings:

python
Copy
Edit
TABLE_NAME = "Movies"  # Your actual DynamoDB table name
AWS_REGION = "us-west-2"  # Change if using a different AWS region
4. Input Data
Ensure that the dynamodb_movies.json file contains the correct structure as expected by the script. The movie data should be structured as a list of items under the Movies key.

Usage
Once the setup is complete, you can run the script to process and batch write data to DynamoDB.

Running the Script
To execute the script, use the following command:

bash
Copy
Edit
python bwriter.py
What Happens When You Run the Script
Data Loading: The script loads the movie data from the JSON file (dynamodb_movies.json).
Data Cleaning: The script removes duplicate entries based on the partition key (tconst), cleans empty string values in numeric fields, and ensures correct formatting for nested attributes like crew.
Denormalization: The script ensures nested attributes (such as crew) are properly formatted for DynamoDB.
Batch Write: The data is split into chunks of 25 items (the DynamoDB batch limit), and then it is written to the specified DynamoDB table in batches. If any items fail to write, the script retries writing them.
Output
The script will output a success message once all the data has been successfully uploaded to DynamoDB:

bash
Copy
Edit
✅ Data upload completed successfully!
Error Handling
If any errors occur during the batch write (such as incorrect data types or unprocessed items), the script will handle them and attempt retries. Check the console output for any error messages or retries.

Code Overview
bwriter.py
This is the main script responsible for:

Loading the TSV data file (dynamodb_movies.json).
Cleaning the data (removing duplicates and cleaning empty values).
Formatting the data to match DynamoDB requirements (ensuring attributes are in the correct format).
Writing data to DynamoDB in batches of 25 items.
Functions in the Script
remove_duplicates(data): Removes duplicate entries based on the tconst partition key.
chunk_data(items, chunk_size=25): Splits the data into smaller chunks (up to 25 items per chunk).
clean_item(item): Cleans individual items by removing empty values and ensuring proper string formatting for certain attributes (e.g., primaryTitle, originalTitle).
fix_crew_format(item): Ensures that the crew attribute is formatted correctly for DynamoDB (converts lists to DynamoDB-compatible formats).
batch_write(items): Handles the batch writing of data to DynamoDB, with automatic retries for unprocessed items.
Contributing
Feel free to contribute to this project! If you find any bugs or have suggestions for improvements, open an issue or submit a pull request.

License
This project is licensed under the MIT License - see the LICENSE file for details.
