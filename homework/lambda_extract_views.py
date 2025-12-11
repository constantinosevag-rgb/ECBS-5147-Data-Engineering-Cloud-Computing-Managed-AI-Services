import datetime
import json

import boto3
import requests

S3_WIKI_BUCKET = "konstantinos-wikidata"
# ---------------------


def lambda_handler(event, context):
    """
    Lambda handler for Wikipedia Page Views ETL pipeline.
    """
    # 1. Get date from event, or default to 21 days ago
    date_str = event.get("date")
    if date_str:
        date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    else:
        # Use timezone aware object for calculation
        date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=21)

    # 2. Extract: fetch from Wikipedia API
    # URL for Page Views
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/{date.strftime('%Y/%m/%d')}"

    # User-Agent header is required by Wikipedia
    headers = {"User-Agent": "WikiViewsPipeline/1.0 (student@example.com)"}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Wikipedia API error: {response.status_code} - {response.text}")

    # 3. Transform: convert to JSON Lines
    try:
        # The structure for views is items -> articles
        top_views = response.json()["items"][0]["articles"]
    except (KeyError, IndexError):
        top_views = []

    current_time = datetime.datetime.now(datetime.timezone.utc)

    json_lines = ""
    for page in top_views:
        record = {
            "title": page.get("article"),
            "views": page.get("views"),
            "rank": page.get("rank"),
            "date": date.strftime("%Y-%m-%d"),
            "retrieved_at": current_time.replace(tzinfo=None).isoformat(),
        }
        json_lines += json.dumps(record) + "\n"

    # 4. Load: upload to S3
    s3 = boto3.client("s3")
    s3_key = f"raw-views/raw-views-{date.strftime('%Y-%m-%d')}.json"

    s3.put_object(Bucket=S3_WIKI_BUCKET, Key=s3_key, Body=json_lines)

    return {
        "statusCode": 200,
        "body": f"Uploaded {len(top_views)} records to s3://{S3_WIKI_BUCKET}/{s3_key}",
    }
