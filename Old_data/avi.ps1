# Import the AWS DynamoDB module
Import-Module AWS.Tools.DynamoDBv2

# Read the JSON file
$json = Get-Content -Raw -Path "test.json" | ConvertFrom-Json

# Loop through each JSON object
foreach ($item in $json) {
    # Handle missing values
    $isAdult = if ($item.isAdult -eq "") { "0" } else { $item.isAdult }
    $startYear = if ($item.startYear -eq "") { "0" } else { $item.startYear }
    $endYear = if ($item.endYear -eq "") { "0" } else { $item.endYear }
    $runtimeMinutes = if ($item.runtimeMinutes -eq "") { "0" } else { $item.runtimeMinutes }
    $genres = if ($item.genres -eq "") { "Unknown" } else { $item.genres }

    # Create a DynamoDB item object
    $ddbItem = @{
        "tconst"         = @{ "S" = "$($item.tconst)" }
        "titleType"      = @{ "S" = "$($item.titleType)" }
        "primaryTitle"   = @{ "S" = "$($item.primaryTitle)" }
        "originalTitle"  = @{ "S" = "$($item.originalTitle)" }
        "isAdult"        = @{ "N" = "$isAdult" }
        "startYear"      = @{ "N" = "$startYear" }
        "endYear"        = @{ "N" = "$endYear" }
        "runtimeMinutes" = @{ "N" = "$runtimeMinutes" }
        "genres"         = @{ "S" = "$genres" }
    }

    # Upload the item to DynamoDB
    try {
        Write-DDBItem -TableName "Movies" -Item $ddbItem
        Write-Host "Uploaded item: $($item.tconst)"
    } catch {
        Write-Host "Failed to upload item: $($item.tconst)"
        Write-Host "Error: $_"
    }
}