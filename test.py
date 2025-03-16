import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import storage

# Load environment variables from .env file
load_dotenv()

# Get the start and end dates from the environment
start_date_str = os.getenv("START_DATE")
end_date_str = os.getenv("END_DATE")

if not start_date_str or not end_date_str:
    raise ValueError("Both START_DATE and END_DATE must be defined in the .env file.")

# Convert the string dates into date objects (assumes format YYYY-MM-DD)
start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

if start_date > end_date:
    raise ValueError("START_DATE must be before or equal to END_DATE.")

# Set up the Google Cloud Storage client
client = storage.Client()
bucket_name = "cryptoe-raw-trade-data"
bucket = client.bucket(bucket_name)
prefix = "trades/"

# List all blobs under the specified prefix
blobs = bucket.list_blobs(prefix=prefix)

# Extract available dates from filenames (expected format: 'YYYY-MM-DD.parquet')
available_dates = set()
for blob in blobs:
    # Remove the prefix to get the filename
    filename = blob.name[len(prefix):]
    if filename.endswith(".parquet"):
        date_str = filename.replace(".parquet", "")
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
            available_dates.add(date_obj)
        except ValueError:
            # Skip files that do not match the expected date format
            continue

# Generate the full date range from start_date to end_date (inclusive)
missing_dates = []
current_date = start_date
while current_date <= end_date:
    if current_date not in available_dates:
        missing_dates.append(current_date)
    current_date += timedelta(days=1)

# Print missing dates
if missing_dates:
    print("Missing dates in GCS for the given range:")
    for date in missing_dates:
        print(date)
else:
    print("No missing dates found in the given range.")
