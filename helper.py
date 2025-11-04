import os
import requests
from dotenv import load_dotenv
from datetime import datetime, UTC
import boto3,urllib
from botocore.exceptions import NoCredentialsError
import io
import lzma
load_dotenv()  # Loads environment variables from a .env file
from config import (STORAGE_TYPE, s3_client, SPACES_BUCKET, SPACES_ENDPOINT,
                    db_name, collection_name, collection)

# from utils import print_log

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"

def print_log(msg):
    """Print log with timestamp, color, icons, and separators for lifecycle events."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # auto-detect icon based on keywords
    if "✅" in msg or "Saved" in msg or "connected" in msg:
        icon = f"{GREEN}✅{RESET}"
    elif "❌" in msg or "FAILED" in msg or "error" in msg.lower():
        icon = f"{RED}❌{RESET}"
    elif "⚠️" in msg or "warn" in msg.lower() or "missing" in msg.lower():
        icon = f"{YELLOW}⚠️{RESET}"
    elif "Save" in msg or "saved" in msg.lower():
        icon = f"{CYAN}💾{RESET}"
    elif "Mongo" in msg:
        icon = "🔄"
    elif "Thread" in msg:
        icon = "🧵"
    elif "Spaces" in msg or "Cloud" in msg:
        icon = "☁️"
    else:
        icon = "ℹ️"

    # main log line
    print(f"{timestamp} | {icon} {msg}")

    # separators for readability
    if "Save: Page saved successfully" in msg:
        print(f"{GREEN}{'─'*60}{RESET}")
    elif "Item: END" in msg:
        print(f"{CYAN}{'═'*60}{RESET}")

def compress_html_to_xz(html_content):
    """
    Compress HTML content using XZ (LZMA) in memory.
    
    Args:
        html_content (str or bytes): The raw HTML data to compress.
        
    Returns:
        io.BytesIO: A buffer containing the XZ-compressed data. This buffer is 
        positioned at the beginning, ready to be passed to boto3 S3 upload methods 
        (upload_fileobj or put_object).
    """
    # Ensure we have bytes (encode text to UTF-8 if needed)
    if isinstance(html_content, str):
        html_content = html_content.encode('utf-8')
    
    # Compress the data using XZ format with a reasonable preset (level 6 by default)
    compressed_data = lzma.compress(html_content, format=lzma.FORMAT_XZ, preset=6)
    
    # Wrap compressed bytes in a BytesIO for file-like access (pointer at start)
    return io.BytesIO(compressed_data)

def save_html_file(html_content, unique_id):
    print_log(f"Save: ENTER save_html_file storage='{STORAGE_TYPE}' id='{unique_id}'")
    url_out = None  # <-- new

    if STORAGE_TYPE == "spaces" and s3_client:
        try:
            buffer = compress_html_to_xz(html_content)
            key = f"{db_name}/{collection_name}/{unique_id}.html.xz"
            s3_client.put_object(
                Bucket=SPACES_BUCKET,
                Key=key,
                # Body=html_content.encode("utf-8"),
                # ContentType="text/html"
                Body=buffer.getvalue(),
                ContentType="application/x-xz"
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



def get_proxy_response(input_url, headers):
    token = os.getenv('PROXY_TOKEN')
    if not token:
        raise ValueError("PROXY_TOKEN not found in environment variables.")
    proxyModeUrl = f"http://{token}:geoCode=us@proxy.scrape.do:8080"
    proxies = {
        "http": proxyModeUrl,
        "https": proxyModeUrl,
    }
    response = requests.get(input_url, headers=headers, allow_redirects=True, proxies=proxies, verify=False)
    return response

def get_proxy_api_response(input_url):
    token = os.getenv('PROXY_TOKEN')
    if not token:
        raise ValueError("PROXY_TOKEN not found in environment variables.")
    targetUrl = urllib.parse.quote(input_url)
    geoCode = "us"
    url = "http://api.scrape.do/?token={}&url={}&geoCode={}".format(token, targetUrl, geoCode)
    response = requests.request("GET", url)
    return response

