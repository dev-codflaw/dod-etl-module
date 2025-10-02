import re
import requests
import os
import pymongo
import json
import json_repair
import hashlib
import datetime
from datetime import datetime, UTC
from parsel import Selector
from threading import Thread
import random
import time
from datetime import datetime, timezone
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from urllib.parse import quote_plus
from dotenv import load_dotenv

from helper import get_proxy_response

load_dotenv()

import boto3
from botocore.exceptions import NoCredentialsError
# ---------------------------------------

# Simple print logger (timestamped)

# ---------------------------------------
from helper import print_log


# Storage type: choose "local" or "spaces"
STORAGE_TYPE = os.getenv("STORAGE_TYPE", "local").lower()

print_log(f"Config: STORAGE_TYPE={STORAGE_TYPE}")



# DigitalOcean Spaces config
SPACES_KEY = os.getenv("DO_SPACES_KEY")
SPACES_SECRET = os.getenv("DO_SPACES_SECRET")
SPACES_REGION = os.getenv("DO_SPACES_REGION", "nyc3")
SPACES_BUCKET = os.getenv("DO_SPACES_BUCKET")
SPACES_ENDPOINT = os.getenv("DO_SPACES_ENDPOINT", f"https://{SPACES_REGION}.digitaloceanspaces.com")

s3_client = None
if STORAGE_TYPE == "spaces" and SPACES_KEY and SPACES_SECRET and SPACES_BUCKET:
    try:
        s3_client = boto3.client(
            "s3",
            region_name=SPACES_REGION,
            endpoint_url=SPACES_ENDPOINT,
            aws_access_key_id=SPACES_KEY,
            aws_secret_access_key=SPACES_SECRET
        )
        print_log(f"Spaces: client initialized for bucket='{SPACES_BUCKET}' region='{SPACES_REGION}' endpoint='{SPACES_ENDPOINT}'")
    except Exception as e:
        print_log(f"Spaces: initialization FAILED -> {e}")
else:
    if STORAGE_TYPE == "spaces":
        print_log("Spaces: missing credentials or bucket env vars. Uploads will NOT work.")
    else:
        print_log("Storage: using LOCAL filesystem.")

username = os.getenv("MONGO_USERNAME")
password = os.getenv("MONGO_PASSWORD")
cluster_url = os.getenv("MONGO_CLUSTER_URL", "cluster1.c4idkzi.mongodb.net")

if not username or not password:
    raise ValueError("MongoDB credentials not set in environment variables.")

uri = f"mongodb+srv://{username}:{quote_plus(password)}@{cluster_url}/"
print_log(f"Mongo: connecting to cluster='{cluster_url}'")
client = pymongo.MongoClient(uri)

# Optional: ping to confirm connection
try:
    client.admin.command("ping")
    print_log("Mongo: connected ✅ (ping ok)")
except Exception as e:
    print_log(f"Mongo: connection FAILED ❌ -> {e}")
    raise

db_name = os.getenv("MONGO_DB_NAME")
collection_name = os.getenv("MONGO_COLLECTION_NAME")
print_log(f"Mongo: using db='{db_name}', input collection='{collection_name}', output collection='output_{collection_name}'")
db = client[db_name]
collection = db[collection_name]
pdp_data = db['output_'+collection_name]
print_log("Mongo: ensuring unique index on output collection (hash_id)")
create_index = pdp_data.create_index('hash_id', unique=True)
print_log(f"Mongo: index ensured -> {create_index}")


def save_html_file(html_content, html_file_path, unique_id):
    """Save HTML either locally or to DigitalOcean Spaces based on STORAGE_TYPE"""
    print_log(f"Save: ENTER save_html_file storage='{STORAGE_TYPE}' id='{unique_id}'")
    url_out = None  # <-- new
    if STORAGE_TYPE == "local":
        try:
            os.makedirs(os.path.dirname(html_file_path), exist_ok=True)
            with open(html_file_path, "w", encoding="utf-8") as file:
                file.write(html_content)
            print_log(f"Save: ✅ Saved locally -> {html_file_path}")
            url_out = html_file_path   # <-- return local path
        except Exception as e:
            print_log(f"Save: ❌ Local save failed -> {e}")

    elif STORAGE_TYPE == "spaces" and s3_client:
        try:
            key = f"{db_name}/{collection_name}/{unique_id}.html"
            s3_client.put_object(
                Bucket=SPACES_BUCKET,
                Key=key,
                Body=html_content.encode("utf-8"),
                ContentType="text/html"
            )
            url_out = f"{SPACES_ENDPOINT}/{SPACES_BUCKET}/{key}"   # <-- return CDN URL
            print(f"☁️ Saved to Spaces: {url_out}")
        except NoCredentialsError:
            print_log("Save: ❌ Spaces upload failed (No credentials)")
        except Exception as e:
            print_log(f"Save: ❌ Spaces upload error -> {e}")

    else:
        print_log("Save: ⚠️ No valid storage method configured.")
    print_log(f"Save: EXIT save_html_file id='{unique_id}'")
    return url_out   # <-- return URL/path


def facebook_scraper(a,b):
    print_log(f"Scraper: ENTER facebook_scraper skip={a} limit={b}")
    print_log("Scraper: fetching pending URLs from Mongo…")
    all_data = collection.find({'status': "pending"}).skip(a).limit(b)
    html_response = None
    for url in all_data:
        input_url = url[os.getenv("MONGO_URL_FIELD", "url")]
        idd = url[os.getenv("MONGO_URL_ID_FIELD", "url_id")]
        print_log(f"Item: START id='{idd}' url='{input_url}'")
        headers1 = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            'dpr': '1',
            'priority': 'u=0, i',
            'sec-ch-prefers-color-scheme': 'dark',
            'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
            'sec-ch-ua-full-version-list': '"Chromium";v="140.0.7339.208", "Not=A?Brand";v="24.0.0.0", "Google Chrome";v="140.0.7339.208"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-model': '""',
            'sec-ch-ua-platform': '"Windows"',
            'sec-ch-ua-platform-version': '"19.0.0"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
            'viewport-width': '1920',
            'cookie': 'sb=lZ7XaNzny4EbZaVe2SIiPEKD; datr=lZ7XaOxRhPU8wKrY4Lh717Ru; c_user=100082945482980; presence=C%7B%22t3%22%3A%5B%5D%2C%22utc3%22%3A1758988798474%2C%22v%22%3A1%7D; fr=1eqoRbxO8CodcGUmC.AWcAzi2wvHo5JVHfsCytUdm3eJTPqQ1V6xFXhigCc4Yj2ydCDm4.Bo2AoA..AAA.0.0.Bo2AoA.AWeDPzzBdHeH6TU1eAoSlkfIDCc; xs=10%3AmAxieJ0ScZZ1LA%3A2%3A1758961362%3A-1%3A-1%3A%3AAcVxBvbwcVzeAOxu4VcaYnokpxRQ_f-Fo0de8Lp8sA; wd=1920x571',
        }

        html_base_path = os.getenv("HTML_BASE_PATH")
        html_path = os.path.join(html_base_path, f"output_{collection_name}/")

        if not os.path.exists(html_path):
            os.makedirs(html_path)
            print_log(f"FS: created local output dir -> {html_path}")

        unique_id = idd
        html_file_path = f'{html_path}{unique_id}.html'
        # Local cache hit only makes sense if STORAGE_TYPE == local
        if os.path.exists(html_file_path) and STORAGE_TYPE == "local":
            print_log("Cache: local file exists, reading instead of hitting proxy")
            print_log(f"Cache: path='{html_file_path}' url='{input_url}'")

            with open(html_file_path, 'r', encoding='utf-8') as file:
                html_response = file.read()
        else:
            print_log(f"Proxy: dispatch -> {input_url}")
            try:
                start_time = time.time()
                response = get_proxy_response(input_url, headers1)
                duration = round(time.time() - start_time, 2)  # seconds
                print_log(f"Proxy: response received status={response.status_code} in {duration}s")
                # store duration in Mongo for tracking
                collection.update_one({'url_id': idd}, {'$set': {'last_fetch_time': duration}})
                collection.update_one({'url_id': idd},{'$set': {'last_fetch_time': duration, 'last_attempt': datetime.now(timezone.utc)}})
            except Exception as e:
                print_log(f"Proxy: request FAILED -> {e}")
                collection.update_one({'url_id': idd}, {'$set': {'status': 'proxy_failed'}})
                print_log(f"Mongo: input status updated -> proxy_failed (id='{idd}')")
                print_log(f"Item: END (proxy_failed) id='{idd}'")
                continue

            if response.status_code == 200 and ('profile_tile_section_type' in response.text or
                                                'full_address' in response.text or
                                                'follower_count' in response.text):
                html_response = response.text  
                print_log("Save: start saving HTML (local/spaces)")
                stored_url = save_html_file(html_response, html_file_path, unique_id)  # get returned path/URL
                print_log("Save: Page saved successfully")
                collection.update_one(
                    {"url_id": idd},
                    {
                        "$set": {
                            "status": "done",              # new status
                            "page_saved": True,            # explicitly mark saved
                            "page_processed": False,       # ready for next pipeline
                            "file_url": stored_url,        # local path or CDN URL
                            "last_success": datetime.now(timezone.utc)
                        },
                        "$inc": {"hit_count": 1}
                    }
                )
                print_log(f"Mongo: status=done, page_saved=True, page_processed=False (id='{idd}')")

            elif response.status_code == 200 and "When this happens, it\'s usually because the owner only shared it with\\n        a small group of people, changed who can see it or it\'s been deleted." in response.text:
                collection.update_one(
                    {"url_id": idd},
                    {
                        "$set": {"status": "error"},
                        "$push": {"error_log": {
                            "time": datetime.now(timezone.utc),
                            "error": "proxy_failed"
                        }},
                        "$inc": {"hit_count": 1}
                    }
                )
                print_log(f"Status: Not Available Page -> {input_url}")
                print_log(f"Mongo: input status updated -> not_available (id='{idd}')")
                print_log(f"Item: END (not_available) id='{idd}'")
                continue
            elif response.status_code == 404 or "This page isn&#039;t available" in response.text:
                collection.update_one(
                    {"url_id": idd},
                    {
                        "$set": {"status": "error"},
                        "$push": {"error_log": {
                            "time": datetime.now(timezone.utc),
                            "error": "not_available"
                        }},
                        "$inc": {"hit_count": 1}
                    }
                )
                print_log("Status: Not Found Page")
                print_log(f"Mongo: input status updated -> not_found (id='{idd}')")
                print_log(f"Item: END (not_found) id='{idd}'")
                continue
            elif response.status_code == 502 or response.status_code == 500:
                collection.update_one(
                    {"url_id": idd},
                    {
                        "$set": {"status": "error"},
                        "$push": {"error_log": {
                            "time": datetime.now(timezone.utc),
                            "error": "not_found"
                        }},
                        "$inc": {"hit_count": 1}
                    }
                )
                print_log("Status: Bad Gateway")
                print_log(f"Mongo: input status updated -> bad_gateway (id='{idd}')")
                print_log(f"Item: END (bad_gateway) id='{idd}'")
                continue
            else:
                collection.update_one(
                    {"url_id": idd},
                    {
                        "$set": {"status": "error"},
                        "$push": {"error_log": {
                            "time": datetime.now(timezone.utc),
                            "error": "bad_gateway"
                        }},
                        "$inc": {"hit_count": 1}
                    }
                )
                print_log(f"Status: Unable to retrieve page, code={response.status_code}")
                print_log(f"Mongo: input status updated -> not_available (id='{idd}')")
                print_log(f"Item: END (not_available) id='{idd}'")
                continue


if __name__ == '__main__':
    run_count = 0
    THREAD_COUNT = int(os.getenv("THREAD_COUNT"))  # Get thread count from env, default 10
    while collection.count_documents({'status': 'pending'}) != 0 and run_count < 10:
        total_count = collection.count_documents({'status': 'pending'})
        variable_count = max(total_count // THREAD_COUNT, 1)
        threads = [Thread(target=facebook_scraper, args=(i, variable_count)) for i in
                   range(0, total_count, variable_count)]
        print_log(f"Threading: Starting {len(threads)} threads for this batch (THREAD_COUNT={THREAD_COUNT})")
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        run_count += 1




