import time
from datetime import datetime, timezone
from utils import print_log
from config import collection
from helper import get_proxy_api_response, save_html_file


def _mark_error(idd, error_key, message):
    """Update Mongo with an error status and log the outcome."""
    collection.update_one(
        {"url_id": idd},
        {
            "$set": {"status": "error"},
            "$push": {
                "error_log": {
                    "time": datetime.now(timezone.utc),
                    "error": error_key,
                }
            },
            "$inc": {"hit_count": 1},
        },
    )
    print_log(message)
    print_log(f"Mongo: input status updated -> {error_key} (id='{idd}')")
    print_log(f"Item: END ({error_key}) id='{idd}'")


def fb_page_save(idd, input_url):
    print_log(f"Proxy: dispatch -> {input_url}")
    try:
        start_time = time.time()
        response = get_proxy_api_response(input_url)
        duration = round(time.time() - start_time, 2)
    except Exception as e:
        print_log(f"Proxy: request FAILED -> {e}")
        collection.update_one({'url_id': idd}, {'$set': {'status': 'proxy_failed'}})
        print_log(f"Mongo: input status updated -> proxy_failed (id='{idd}')")
        print_log(f"Item: END (proxy_failed) id='{idd}'")
        return None

    print_log(f"Proxy: response received status={response.status_code} in {duration}s")
    collection.update_one(
        {"url_id": idd},
        {
            "$set": {
                "last_fetch_time": duration,
                "last_attempt": datetime.now(timezone.utc),
            }
        },
    )

    html_text = response.text

    if response.status_code == 200 and any(
        marker in html_text
        for marker in ("profile_tile_section_type", "full_address", "follower_count")
    ):
        html_response = html_text
        print_log("Save: start saving HTML (s3 spaces)")
        save_html_file(html_response, idd)
        print_log("Save: Page saved successfully")
        return html_response

    if response.status_code == 200 and (
        "When this happens, it's usually because the owner only shared it"
        " with\\n        a small group of people, changed who can see it or it's been deleted."
        in html_text
    ):
        _mark_error(idd, "not_available", f"Status: Not Available Page -> {input_url}")
        return None

    if response.status_code == 404 or "This page isn&#039;t available" in html_text:
        _mark_error(idd, "not_found", "Status: Not Found Page")
        return None

    if response.status_code in (500, 502):
        _mark_error(idd, "bad_gateway", "Status: Bad Gateway")
        return None

    _mark_error(
        idd,
        "not_available",
        f"Status: Unable to retrieve page, code={response.status_code}",
    )
    return None
