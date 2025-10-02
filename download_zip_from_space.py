import boto3
import os
import zipfile

# --- Config ---
SPACES_KEY = os.getenv("DO_SPACES_KEY")
SPACES_SECRET = os.getenv("DO_SPACES_SECRET")
SPACES_REGION = os.getenv("DO_SPACES_REGION", "nyc3")
SPACES_BUCKET = os.getenv("DO_SPACES_BUCKET")
SPACES_ENDPOINT = os.getenv("DO_SPACES_ENDPOINT", f"https://{SPACES_REGION}.digitaloceanspaces.com")

DOWNLOAD_DIR = "./spaces_downloads"
ZIP_FILE = "spaces_files.zip"

# --- Client ---
s3 = boto3.client(
    "s3",
    region_name=SPACES_REGION,
    endpoint_url=SPACES_ENDPOINT,
    aws_access_key_id=SPACES_KEY,
    aws_secret_access_key=SPACES_SECRET
)

# --- Step 1: Download all files ---
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
paginator = s3.get_paginator("list_objects_v2")

print("📥 Downloading all files from Spaces...")
for page in paginator.paginate(Bucket=SPACES_BUCKET):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        local_path = os.path.join(DOWNLOAD_DIR, key)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        print(f"⬇️ {key} -> {local_path}")
        s3.download_file(SPACES_BUCKET, key, local_path)

# --- Step 2: Zip with max compression ---
print("📦 Creating ZIP archive...")
with zipfile.ZipFile(ZIP_FILE, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
    for root, _, files in os.walk(DOWNLOAD_DIR):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, DOWNLOAD_DIR)
            zf.write(full_path, arcname=rel_path)

print(f"✅ All files zipped -> {ZIP_FILE}")
