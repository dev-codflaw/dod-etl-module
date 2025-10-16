import os
from parsel import Selector
from threading import Thread
import urllib3
import sys
from datetime import datetime, timezone, UTC
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from utils import c_replace, get_useragent, clean_url
from urllib.parse import quote_plus
from dotenv import load_dotenv
from helper import get_proxy_response, get_proxy_api_response
from save import fb_page_save
load_dotenv()
from utils import print_log
from helper import save_html_file
from config import (STORAGE_TYPE, s3_client, SPACES_BUCKET, SPACES_ENDPOINT,
                    db_name, collection_name, collection, pdp_data)
from process import fb_process

def facebook_scraper(a,b):
    print_log(f"Scraper: ENTER facebook_scraper skip={a} limit={b}")
    print_log("Scraper: fetching pending URLs from Mongo…")
    all_data = collection.find({'status': "pending"}).skip(a).limit(b)
    html_response = None
    for url in all_data:
        input_url = url[os.getenv("MONGO_URL_FIELD", "url")]
        idd = url[os.getenv("MONGO_URL_ID_FIELD", "url_id")]
        country = "NA"
        try:
            country = url["country"]
        except Exception as e:
            print_log(f"Country not found in document, setting to 'Unknown'. Error: {e}")

        print_log(f"Item: START id='{idd}' url='{input_url}'")

        html_base_path = os.getenv("HTML_BASE_PATH")

        if not html_base_path:
            print_log(
                "Configuration error: HTML_BASE_PATH is not set. "
                "Set the environment variable to a writable directory, "
                "for example by running 'export HTML_BASE_PATH=/path/to/storage'."
            )
            return

        html_base_path = os.path.abspath(html_base_path)

        if not os.path.exists(html_base_path):
            try:
                os.makedirs(html_base_path, exist_ok=True)
            except OSError as exc:
                print_log(
                    "Configuration error: Unable to create the directory specified by "
                    "HTML_BASE_PATH. Set it to a writable directory, for example by "
                    "running 'export HTML_BASE_PATH=/path/to/storage'. "
                    f"Original error: {exc}"
                )
                return

        if not os.path.isdir(html_base_path) or not os.access(html_base_path, os.W_OK):
            print_log(
                "Configuration error: HTML_BASE_PATH must point to a writable directory. "
                "Set it to a writable location, for example by running "
                "'export HTML_BASE_PATH=/path/to/storage'."
            )
            return

        html_path = os.path.join(html_base_path, f"output_{collection_name}/")

        unique_id = idd
        html_file_path = f'{html_path}{unique_id}.html'

        html_response = fb_page_save(html_path, idd, input_url, html_file_path)

        if html_response:
            print_log("Parse: start processing HTML")
            fb_process(html_response, idd, input_url, html_file_path, country=country)
        else:
            print_log("Parse: HTML response is None, skipping processing")
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


