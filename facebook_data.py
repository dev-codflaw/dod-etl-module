import os
from threading import Thread
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import ReturnDocument

from save import fb_page_save
from utils import print_log
from config import collection
from process import fb_process

load_dotenv()


def _claim_next_pending():
    """Atomically fetch the next pending document and mark it as in-progress."""
    return collection.find_one_and_update(
        {"status": "pending"},
        {
            "$set": {
                "status": "in_progress",
                "claimed_at": datetime.now(timezone.utc),
            }
        },
        sort=[("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )


def facebook_scraper(thread_index):
    print_log(f"Thread-{thread_index}: started")

    while True:
        url_doc = _claim_next_pending()
        if not url_doc:
            print_log(f"Thread-{thread_index}: no more pending URLs, exiting")
            break

        url_field = os.getenv("MONGO_URL_FIELD", "url")
        id_field = os.getenv("MONGO_URL_ID_FIELD", "url_id")
        input_url = url_doc.get(url_field)
        idd = url_doc.get(id_field)

        if not input_url or not idd:
            print_log(f"Thread-{thread_index}: missing URL or ID, skipping document {url_doc.get('_id')}")
            continue

        country = url_doc.get("country", "NA")
        if country == "NA":
            print_log("Country not found in document, defaulting to 'NA'")

        print_log(f"Item: START id='{idd}' url='{input_url}' (thread={thread_index})")

        try:
            html_response = fb_page_save(idd, input_url)
            if html_response:
                print_log("Parse: start processing HTML")
                fb_process(html_response, idd, input_url, country=country)
            else:
                print_log("Parse: HTML response is None, skipping processing")
        except Exception as exc:
            print_log(f"Thread-{thread_index}: unexpected error -> {exc}")
            collection.update_one(
                {"url_id": idd},
                {
                    "$set": {"status": "error"},
                    "$push": {
                        "error_log": {
                            "time": datetime.now(timezone.utc),
                            "error": "thread_failure",
                            "details": str(exc),
                        }
                    },
                    "$inc": {"hit_count": 1},
                },
            )
            print_log(f"Item: END (thread_failure) id='{idd}'")


if __name__ == '__main__':
    thread_count = int(os.getenv("THREAD_COUNT", "10"))
    pending_total = collection.count_documents({'status': 'pending'})
    print_log(f"Main: pending URLs={pending_total}, THREAD_COUNT={thread_count}")

    threads = [
        Thread(target=facebook_scraper, args=(index,))
        for index in range(thread_count)
    ]

    for th in threads:
        th.start()
    for th in threads:
        th.join()

