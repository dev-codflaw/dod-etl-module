import os
from threading import Thread
from queue import Queue
from datetime import datetime, timezone

from dotenv import load_dotenv

from save import fb_page_save
from utils import print_log
from config import collection
from process import fb_process

load_dotenv()

URL_FIELD = os.getenv("MONGO_URL_FIELD", "url")
ID_FIELD = os.getenv("MONGO_URL_ID_FIELD", "url_id")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))


def _claim_batch(batch_size):
    """Claim a limited batch of pending documents and mark them in-progress."""
    projection = {URL_FIELD: 1, ID_FIELD: 1, "country": 1}
    docs = list(
        collection.find({"status": "pending"}, projection=projection)
        .sort("_id", 1)
        .limit(batch_size)
    )
    if not docs:
        return []

    ids = [doc["_id"] for doc in docs]
    now = datetime.now(timezone.utc)
    collection.update_many(
        {"_id": {"$in": ids}, "status": "pending"},
        {"$set": {"status": "in_progress", "claimed_at": now}},
    )
    in_progress_ids = {
        doc["_id"]
        for doc in collection.find(
            {"_id": {"$in": ids}, "status": "in_progress"},
            {"_id": 1},
        )
    }

    claimed_docs = []
    for doc in docs:
        if doc["_id"] in in_progress_ids:
            doc["status"] = "in_progress"
            doc["claimed_at"] = now
            claimed_docs.append(doc)
    return claimed_docs


def facebook_worker(thread_index, task_queue):
    print_log(f"Thread-{thread_index}: started")

    while True:
        doc = task_queue.get()
        if doc is None:
            print_log(f"Thread-{thread_index}: shutdown signal received")
            task_queue.task_done()
            break

        input_url = doc.get(URL_FIELD)
        idd = doc.get(ID_FIELD)

        if not input_url or not idd:
            print_log(f"Thread-{thread_index}: missing URL or ID, skipping document {doc.get('_id')}")
            task_queue.task_done()
            continue

        country = doc.get("country", "NA") or "NA"
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

        task_queue.task_done()


def _dispatch_batches(task_queue, thread_count):
    """Load batches into the task queue, waiting for each batch to finish."""
    batch_number = 0
    while True:
        batch = _claim_batch(BATCH_SIZE)
        if not batch:
            break
        batch_number += 1
        print_log(f"Batch: dispatching #{batch_number} with {len(batch)} items")
        for doc in batch:
            task_queue.put(doc)
        task_queue.join()

    print_log("Batch: no more pending documents to dispatch")
    for _ in range(thread_count):
        task_queue.put(None)


if __name__ == '__main__':
    thread_count = int(os.getenv("THREAD_COUNT", "10"))
    pending_total = collection.count_documents({'status': 'pending'})
    print_log(
        f"Main: pending URLs={pending_total}, THREAD_COUNT={thread_count}, BATCH_SIZE={BATCH_SIZE}"
    )

    task_queue = Queue()
    threads = [
        Thread(target=facebook_worker, args=(index, task_queue))
        for index in range(thread_count)
    ]

    for th in threads:
        th.start()

    _dispatch_batches(task_queue, thread_count)

    for th in threads:
        th.join()
