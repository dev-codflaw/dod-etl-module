import re


from datetime import datetime, UTC
from parsel import Selector
from threading import Thread
import random


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

headers1 = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'dpr': '1',
    'priority': 'u=0, i',
    'sec-ch-prefers-color-scheme': 'dark',
    'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    'sec-ch-ua-full-version-list': '"Chromium";v="140.0.7339.208", "Not=A?Brand";v="24.0.0.0", "Google Chrome";v="140.0.7339.208"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"19.0.0"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
    'viewport-width': '1920',
    'cookie': 'sb=lZ7XaNzny4EbZaVe2SIiPEKD; datr=lZ7XaOxRhPU8wKrY4Lh717Ru; c_user=100082945482980; presence=C%7B%22t3%22%3A%5B%5D%2C%22utc3%22%3A1758988798474%2C%22v%22%3A1%7D; fr=1eqoRbxO8CodcGUmC.AWcAzi2wvHo5JVHfsCytUdm3eJTPqQ1V6xFXhigCc4Yj2ydCDm4.Bo2AoA..AAA.0.0.Bo2AoA.AWeDPzzBdHeH6TU1eAoSlkfIDCc; xs=10%3AmAxieJ0ScZZ1LA%3A2%3A1758961362%3A-1%3A-1%3A%3AAcVxBvbwcVzeAOxu4VcaYnokpxRQ_f-Fo0de8Lp8sA; wd=1920x571',
}

        
def c_replace(html=''):
    """
        This method for some more than replace method need to apply in
        one string so in this method customize all replace like \n \r \t or extra space
        all are replce and return proper string.
    """

    if isinstance(html, str):
        html = html.replace("&gt;", ">")
        html = html.replace("&lt;", "<")
        html = html.replace("&amp;", "&")
        html = html.replace("\r\n", " ")
        html = html.replace("", "")
        # html = html.replace(vbLf, " ").replace(vbCrLf, " ").replace(vbCr, " ")
        html = html.replace("\t", " ")
        html = html.replace("\n", " ")
        html = html.replace("\r", " ")
        html = html.replace("&nbsp;", " ")
        html = re.sub("<script[^>]*>([\w\W]*?)</script>", " ", html)
        html = re.sub("\ style specs start[^>]*>([\w\W]*?)style specs end ", " ", html)
        html = re.sub("<style[^>]*>([\w\W]*?)</style>", " ", html)
        html = re.sub("<!--([\w\W]*?)-->", " ", html)
        html = re.sub("<([\w\W]*?)>", " ", html)
        html = re.sub("<.*?>", " ", html)
        # html = str(emoji.get_emoji_regexp().sub(u'', html))
        html = re.sub(" +", " ", html)
        return html.strip()

    elif isinstance(html, list):
        return [j for j in [c_replace(i) for i in html] if j]
    else:
        raise TypeError(f'must be str or list - object pass is ({type(html)}) object....')

def get_useragent():
    windows_user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Edg/139.0.3405.102",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:141.0) Gecko/20100101 Firefox/141.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 OPR/120.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:138.0) Gecko/20100101 Firefox/138.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/127.0.2651.105",
        "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; rv:57.0) Gecko/20100101 Firefox/57.0",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:52.0) Gecko/20100101 Firefox/52.0",
        "Mozilla/5.0 (Windows NT 5.1; rv:52.0) Gecko/20100101 Firefox/52.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.2849.52 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/128.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36 Edg/101.0.1210.47",
        "Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko",
        "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 OPR/49.0.2725.64",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:56.0) Gecko/20100101 Firefox/56.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:57.0) Gecko/20100101 Firefox/57.0",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0",
        "Mozilla/5.0 (Windows NT 6.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.0; WOW64; rv:57.0) Gecko/20100101 Firefox/57.0",
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299",
        "Mozilla/5.0 (Windows NT 5.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
        "Mozilla/5.0 (Windows NT 5.1; WOW64; rv:52.0) Gecko/20100101 Firefox/52.0",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; ru; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16 (.NET CLR 3.5.30729; .NET4.0E)",
        "Mozilla/5.0 (Windows NT 5.1; rv:52.0) Gecko/20100101 Firefox/52.0",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
    ]
    return random.choice(windows_user_agents)



def clean_url(url: str) -> str:
    """
    Cleans and normalizes a single URL.
    - Adds https://www. if missing
    - Removes existing http/https and www prefixes
    - Converts to lowercase
    """
    if not url:
        return ""

    url = url.strip().lower()

    # Remove existing http://, https://, and www.
    url = re.sub(r"^https?://(www\.)?", "", url)
    url = re.sub(r"^www\.", "", url)

    # Add standard https://www. prefix
    url = "https://www." + url

    return url