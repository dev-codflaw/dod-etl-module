import os
import time
from datetime import datetime, timezone

from utils import print_log
from config import (STORAGE_TYPE, s3_client, SPACES_BUCKET, SPACES_ENDPOINT,
                    db_name, collection_name, collection, pdp_data)

from helper import get_proxy_api_response, save_html_file

def fb_page_save(html_path, idd, input_url, html_file_path):
    unique_id = idd
    if not os.path.exists(html_path):
        os.makedirs(html_path)
        print_log(f"FS: created local output dir -> {html_path}")

    # Local cache hit only makes sense if STORAGE_TYPE == local
    if os.path.exists(html_file_path) and STORAGE_TYPE == "local":
        print_log("Cache: local file exists, reading instead of hitting proxy")
        print_log(f"Cache: path='{html_file_path}' url='{input_url}'")

        with open(html_file_path, 'r', encoding='utf-8') as file:
            html_response = file.read()
            return html_response
    else:
        print_log(f"Proxy: dispatch -> {input_url}")
        try:
            start_time = time.time()
            response = get_proxy_api_response(input_url)
            duration = round(time.time() - start_time, 2)  # seconds
            print_log(f"Proxy: response received status={response.status_code} in {duration}s")
            # store duration in Mongo for tracking
            collection.update_one({'url_id': idd},{'$set': {'last_fetch_time': duration, 'last_attempt': datetime.now(timezone.utc)}})
        except Exception as e:
            print_log(f"Proxy: request FAILED -> {e}")
            collection.update_one({'url_id': idd}, {'$set': {'status': 'proxy_failed'}})
            print_log(f"Mongo: input status updated -> proxy_failed (id='{idd}')")
            print_log(f"Item: END (proxy_failed) id='{idd}'")

        if response.status_code == 200 and ('profile_tile_section_type' in response.text or
                                                'full_address' in response.text or
                                                'follower_count' in response.text):
            html_response = response.text  
            print_log("Save: start saving HTML (local/spaces)")
            save_html_file(html_response, html_file_path, unique_id)
            print_log("Save: Page saved successfully")
            return html_response

        elif response.status_code == 200 and "When this happens, it\'s usually because the owner only shared it with\\n        a small group of people, changed who can see it or it\'s been deleted." in response.text:
            # collection.update_one({'url_id': idd}, {'$set': {'status': 'not_available'}})
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
        elif response.status_code == 404 or "This page isn&#039;t available" in response.text:
            # collection.update_one({'url_id': idd}, {'$set': {'status': 'not_found'}})
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
        elif response.status_code == 502 or response.status_code == 500:
            # collection.update_one({'url_id': idd}, {'$set': {'status': 'bad_gateway'}})
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
        else:
            # collection.update_one({'url_id': idd}, {'$set': {'status': 'not_available'}})
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
        
