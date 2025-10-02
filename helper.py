import os
import requests
from dotenv import load_dotenv
from datetime import datetime, UTC

load_dotenv()  # Loads environment variables from a .env file

# ---------------------------------------
# Enhanced print logger (timestamped + icons + colors + separators)
# ---------------------------------------

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



