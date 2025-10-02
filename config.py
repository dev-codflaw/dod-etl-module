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
import urllib3
from datetime import datetime, timezone

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

import boto3
from botocore.exceptions import NoCredentialsError
# Storage type: choose "local" or "spaces"
STORAGE_TYPE = os.getenv("STORAGE_TYPE", "local").lower()

from utils import print_log

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