#DE2_HW4

# %% imporitng 
import datetime
import json
import os
from pathlib import Path

import boto3
import requests

# %% date time 
DATE_PARAM = "2024-11-15"
#DATE_PARAM = "2024-11-16"
#DATE_PARAM = "2024-11-17"
#DATE_PARAM = "2024-11-18"
date = datetime.datetime.strptime(DATE_PARAM, "%Y-%m-%d")

# %% creating directory 
current_directory = Path(__file__).parent
RAW_LOCATION_BASE = current_directory / "data" / "raw-views"
RAW_LOCATION_BASE.mkdir(exist_ok=True, parents=True)
print(f"Created directory {RAW_LOCATION_BASE}")

# %% call API 
url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia.org/all-access/{date.strftime('%Y/%m/%d')}" 
wiki_server_response = requests.get(url, headers={"User-Agent": "curl/7.68.0"})
wiki_response_status = wiki_server_response.status_code
wiki_response_body = wiki_server_response.text

print(f"Wikipedia REST API Response body: {wiki_response_body}")
print(f"Wikipedia REST API Response Code: {wiki_response_status}")
print(wiki_response_body)

# %% creating and writitng files 
file_name = f"raw-views-{datetime.datetime.strptime(DATE_PARAM, "%Y-%m-%d").date()}.txt"

raw_views_file = os.path.join(RAW_LOCATION_BASE, file_name)
raw_views_file = Path(raw_views_file)
with open(raw_views_file, "w") as file:
     file.write(wiki_response_body)

print(f"File saved at: {raw_views_file}")

# %% checking: Open the file in read mode
try:
    with open(raw_views_file, "r") as file:
        content = file.read()
    print("File content:")
    print(content)
except FileNotFoundError:
    print(f"❌ The file at '{file_path}' does not exist.")
except Exception as e:
    print(f"❌ An error occurred: {str(e)}")

# %% Upload the file to S3
S3_WIKI_BUCKET = "gh25-wikidata"
s3 = boto3.client("s3", region_name="eu-west-1")

s3_key = f"datalake/raw/{file_name}"  # Placeholder, feel free to remove this
s3.put_object(Bucket="gh25-wikidata", Key="datalake/raw/")
s3.put_object(Bucket="gh25-wikidata", Key=f"{s3_key}", Body=open(raw_views_file, "rb"))
print(f"File saved at: {raw_views_file}")
print(f"Uploaded raw edits to s3://{S3_WIKI_BUCKET}/{s3_key}")

# %% Convert the response into a JSON lines formatted file
wiki_response_parsed = wiki_server_response.json()
top_views = wiki_response_parsed["items"][0]["articles"]
current_time = datetime.datetime.now(datetime.timezone.utc)  # Always use UTC!!
json_lines = ""
for page in top_views:
    record = {
        "article": page["article"],
        "views": page["views"],
        "rank": page["rank"],
        "date": date.strftime("%Y-%m-%d"),
        "retrieved_at": current_time.replace(
            tzinfo=None
        ).isoformat(),  # We need to remove tzinfo as Athena cannot work with offsets
    }
    json_lines += json.dumps(record) + "\n"

JSON_LOCATION_DIR = current_directory / "data" / "views"
JSON_LOCATION_DIR.mkdir(exist_ok=True, parents=True)
print(f"Created directory {JSON_LOCATION_DIR}")
print(f"JSON lines:\n{json_lines}")

# %%Upload the JSON lines file you saved locally to S3 into your bucket 
json_lines_filename = f"views-{date.strftime('%Y-%m-%d')}.json"
json_lines_file = JSON_LOCATION_DIR / json_lines_filename

with json_lines_file.open("w") as file:
    file.write(json_lines)

s3.upload_file(json_lines_file, S3_WIKI_BUCKET, f"datalake/views/{json_lines_filename}")
print(f"✅ Uploaded processed data to s3://{S3_WIKI_BUCKET}/datalake/views/{json_lines_filename}")

# %% Making sure the script is “idempotent”

