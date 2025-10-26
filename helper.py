import os
import time

import requests
from dotenv import load_dotenv
from datetime import datetime, UTC
import boto3,urllib
from botocore.exceptions import NoCredentialsError
import random

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

    # --- Cookie pool ---
    cookie_pool = [
        "c_user=61565316136793;xs=15:RYCgPuIRCosJ0w:2:1726339065:-1:-1;fr=0M3QpRW1eBL11WFd8.AWUcJfBvUBns3ANT2JG1FnmrJIU.Bm5df4..AAA.0.0.Bm5df4.AWUwyrdQsTQ;datr=-NflZq5eLO701Ok8_6Vfn-wM;",
        "c_user=61565403946565;xs=7:JTAyts_a5E1Egg:2:1726338399:-1:-1;fr=0OE1x416eyOFk0b8j.AWVVOcDNku39JaZeso6UP1QY5D8.Bm5dVe..AAA.0.0.Bm5dVe.AWVN-5KnEs4;datr=XtXlZjjjL5T1qIAP5INaiGEY;",
        "c_user=61565622819508;xs=47:PTx7uCgU-G4BvA:2:1726337558:-1:-1;fr=0EMr9xmrknjGz9aR8.AWXy20PeT92xjDL2F7WlKEqx7fw.Bm5dIV..AAA.0.0.Bm5dIV.AWVkr9jmx6E;datr=FdLlZlKjyz4KMhhXdIvbCrTB;",
        "c_user=61565706747085;xs=42:ZUQvzimLezybqQ:2:1726336572:-1:-1;fr=0KIta2OVlTusdeeGY.AWXY5MPLR_r1fvr25CV5na3_c6s.Bm5c47..AAA.0.0.Bm5c47.AWWUfTpo9L4;datr=O87lZuBqdL3vWjxBlhk1nYA8;",
        "c_user=61565714607520;xs=23:dlrMWCMJEcNIVA:2:1726335514:-1:-1;fr=0gU1qEWf1e3v4In4X.AWUfzxFYmCzkrCdGS3Y5WxUVlsk.Bm5coa..AAA.0.0.Bm5coa.AWW_tAo07h8;datr=GsrlZtJgulKk5WNXmkWmby_7;",
        "c_user=61565553972189;xs=29:fsIqqXZmH7CbvA:2:1726337849:-1:-1;fr=0GCWlaMTiHZsn2dsa.AWWlzMVkwd7ANJXlq-511IsqaxQ.Bm5dM5..AAA.0.0.Bm5dM5.AWWeHjXwF7I;datr=OdPlZqhGhuCyuNwA1r9HhGA_;",
        "c_user=61565684249801;xs=49:Bl9f4dEqxwG8PA:2:1726339118:-1:-1;fr=02sfL2Wk1gnxPnOIL.AWUs5R796enGPslPkF5qxLDZOJg.Bm5dgu..AAA.0.0.Bm5dgu.AWWWVYzTUSM;datr=LtjlZqPXSU2NDaojp84r_SsX;",
        "c_user=61565670960872;xs=29:M0x4fgmrnGjSOg:2:1726339909:-1:-1;fr=0b2QmPj2yBLKIyBJD.AWXGGGp4pT0gPhUXwGzPlQFvBLE.Bm5dtF..AAA.0.0.Bm5dtF.AWUns8YIAcI;datr=RdvlZiApd-r-CtLoEQ_T6bf4;"
    ]

    # --- Windows user-agents ---
    windows_user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:141.0) Gecko/20100101 Firefox/141.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Edg/139.0.3405.102",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:57.0) Gecko/20100101 Firefox/57.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.2849.52 Safari/537.36",
    ]

    # --- Random selections ---
    selected_cookie = random.choice(cookie_pool)
    selected_user_agent = random.choice(windows_user_agents)

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'max-age=0',
        'dpr': '1.25',
        'priority': 'u=0, i',
        # 'referer': 'https://www.facebook.com/recover/password/?u=100050634553082&n=245068&fl=default_recover&sih=0&ssb=1&msgr=0&rm=send_whatsapp&next=https%3A%2F%2Fwww.facebook.com%2Fpages%2FGuiding%252520Light%252520Baptist%252520Church%2F111610685541650%2F%23',
        'sec-ch-prefers-color-scheme': 'light',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'sec-ch-ua-full-version-list': '"Google Chrome";v="141.0.7390.108", "Not?A_Brand";v="8.0.0.0", "Chromium";v="141.0.7390.108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"19.0.0"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': selected_user_agent,
        # 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
        'viewport-width': '1536',
        'cookie': selected_cookie,
    }

    # sleep_time = random.uniform(5, 10)  # returns a float like 17.482
    # print(f"⏳ Sleeping for {sleep_time:.2f} seconds...")
    # time.sleep(sleep_time)
    response = requests.request("GET", url,headers=headers)
    # if response.status_code != 200:
    return response

