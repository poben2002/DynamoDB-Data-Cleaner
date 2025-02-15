import boto3 
import time
from collections import defaultdict

# Initialize DynamoDB client
dynamodb = boto3.client("dynamodb", region_name="us-west-2")

TABLE_NAME = "Movies"

def query_movies_by_tconst(tconst_list):
    """Queries the Movies table for a list of tconsts."""
    results = []
    for tconst in tconst_list:
        response = dynamodb.get_item(
            TableName=TABLE_NAME,
            Key={"tconst": {"S": tconst}}
        )
        movie = response.get("Item", {})
        if not movie:
            print(f"Warning: No movie found for tconst {tconst}")
        else:
            print(f"Retrieved movie {tconst}: {movie}")
        results.append(movie)
    return results

def scan_movies(projection):
    """Scans the Movies table and retrieves only specified attributes."""
    response = dynamodb.scan(
        TableName=TABLE_NAME,
        ProjectionExpression=projection
    )
    return response.get("Items", [])

def group_by_attribute(items, attribute):
    """Groups and counts occurrences of an attribute, handling missing values."""
    count_dict = defaultdict(int)
    for item in items:
        if attribute == "genres":
            genres = item.get(attribute, {}).get("L", [])
            # Extract genre names from the nested structure
            genre_names = [genre.get("S") for genre in genres if genre.get("S")]
            if not genre_names:  # Skip if no valid genres
                continue
            for genre in genre_names:
                count_dict[genre] += 1
        elif attribute == "startYear":
            # Handle startYear carefully
            start_year = item.get(attribute, {}).get("N", None)
            if start_year is None:  # Skip items with no startYear
                continue
            count_dict[start_year] += 1
    return dict(count_dict)

def get_max(items, attribute):
    """Finds the max value for an attribute."""
    max_value = 0
    max_item = None
    for item in items:
        # Extract the numVotes from ratings safely
        num_votes = item.get('ratings', {}).get('M', {}).get('numVotes', {}).get('N', '0')

        # Ensure num_votes is a valid integer
        if num_votes.isdigit():
            num_votes = int(num_votes)
        else:
            num_votes = 0  # Default to 0 if invalid

        if num_votes > max_value:
            max_value = num_votes
            max_item = item

    return max_item, max_value


def execute_workload(tconst_list):
    start_time = time.time()

    print("Executing Part 1...")
    queried_movies = query_movies_by_tconst(tconst_list)
    print(f"Retrieved {len(queried_movies)} movies.")

    print("Executing Part 2...")
    movies = scan_movies("startYear")
    year_counts = group_by_attribute(movies, "startYear")
    # Ensure that you ignore "N/A" or empty results
    valid_year_counts = {k: v for k, v in year_counts.items() if k != "N/A"}
    max_year, max_count = max(valid_year_counts.items(), key=lambda x: x[1], default=("N/A", 0))
    print(f"Year with most movies: {max_year} ({max_count} movies)")

    print("Executing Part 3...")
    movies = scan_movies("tconst, genres, ratings")
    # Grouping by genre to get the top genres
    genre_counts = group_by_attribute(movies, "genres")
    # Filter out empty genres (if any)
    valid_genre_counts = {k: v for k, v in genre_counts.items() if k != "N/A"}
    sorted_genres = sorted(valid_genre_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    print("Top 10 genres by movie count:", sorted_genres)

    print("Executing Part 4...")
    response = dynamodb.scan(
        TableName=TABLE_NAME,
        ExpressionAttributeNames={"#tt": "titleType", "#sy": "startYear"},
        ExpressionAttributeValues={":type": {"S": "movie"}, ":start": {"N": "2014"}, ":end": {"N": "2024"}},
        FilterExpression="#tt = :type AND #sy BETWEEN :start AND :end",
        ProjectionExpression="tconst, startYear, titleType, runtimeMinutes"
    )
    movies_2014_2024 = response.get("Items", [])

    year_runtime = defaultdict(list)
    for movie in movies_2014_2024:
        year = movie.get("startYear", {}).get("N")
        runtime = int(movie.get("runtimeMinutes", {}).get("N", 0))
        if year:
            year_runtime[year].append(runtime)
    avg_runtime_per_year = {year: sum(rt) / len(rt) for year, rt in year_runtime.items()}
    sorted_avg_runtime = sorted(avg_runtime_per_year.items(), key=lambda x: int(x[0]))
    print("Average runtime per year (2014-2024):", sorted_avg_runtime)

    print("Executing Part 5...")
    movies = scan_movies("tconst, primaryTitle, ratings.numVotes")
    max_movie, max_votes = get_max(movies, "ratings.numVotes")

    if max_movie:
        max_title = max_movie.get("primaryTitle", {}).get("S", "N/A")
        print(f"Most voted movie: {max_title} with {max_votes} votes")
    else:
        print("No movie found with valid vote count.")

    print(f"Total execution time: {time.time() - start_time:.2f} seconds")

# Ensure the script runs when executed directly
if __name__ == "__main__":
    tconst_list = ["tt4555672", "tt2330574", "tt29593596"]  # Example tconsts
    execute_workload(tconst_list)
