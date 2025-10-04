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
from utils import c_replace, get_useragent, clean_url
from urllib.parse import quote_plus
from dotenv import load_dotenv
from helper import get_proxy_response, get_proxy_api_response
from save import fb_page_save
load_dotenv()
import boto3
from botocore.exceptions import NoCredentialsError
from utils import print_log
from helper import save_html_file
from config import (STORAGE_TYPE, s3_client, SPACES_BUCKET, SPACES_ENDPOINT,
                    db_name, collection_name, collection, pdp_data)
from process import fb_process

def facebook_scraper(a,b):
    print_log(f"Scraper: ENTER facebook_scraper skip={a} limit={b}")
    print_log("Scraper: fetching pending URLs from Mongo…")
    all_data = collection.find({'status': "done"}).skip(a).limit(b)
    html_response = None
    for url in all_data:
        input_url = url[os.getenv("MONGO_URL_FIELD", "url")]
        idd = url[os.getenv("MONGO_URL_ID_FIELD", "url_id")]
        print_log(f"Item: START id='{idd}' url='{input_url}'")

        html_base_path = os.getenv("HTML_BASE_PATH")
        html_path = os.path.join(html_base_path, f"output_{collection_name}/")

        unique_id = idd
        html_file_path = f'{html_path}{unique_id}.html'

        html_response = fb_page_save(html_path, idd, input_url, html_file_path)

        if html_response:
            print_log("Parse: start processing HTML")
            fb_process(html_response, idd, input_url, html_file_path)
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


