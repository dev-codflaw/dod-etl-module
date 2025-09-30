import os
import requests
from dotenv import load_dotenv

load_dotenv()  # Loads environment variables from a .env file

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



