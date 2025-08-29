import os, httpx, time
from utils.save_data import save_jsonl
from dotenv import load_dotenv
load_dotenv()

API = os.getenv("GOOGLE_API_KEY")
CX  = os.getenv("GOOGLE_CX")
url = "https://www.googleapis.com/customsearch/v1"

all_items = []
start = 1
while start <= 31:  # 대략 4페이지(최대 10개씩)
    params = {"key": API, "cx": CX, "q": "AI product launch", "num": 10, "start": start, "dateRestrict": "d7"}
    r = httpx.get(url, params=params, timeout=20)
    r.raise_for_status()
    j = r.json()
    items = j.get("items", [])
    if not items: break
    all_items.extend(items)
    start += 10
    time.sleep(0.2)   # 살짝 천천히

# MAIN =============================================================
if __name__ =="__main__":
    save_jsonl(all_items, "results/google_data.jsonl")
