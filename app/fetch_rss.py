import feedparser
import hashlib
from datetime import datetime, timezone
from dateutil import parser as dtparse
from urllib.parse import urlencode
from utils.save_data import save_jsonl
import itertools

# RSS 피드 목록 ===============================================
FEEDS = {
    # 매체 공식 RSS
    "techcrunch": "https://techcrunch.com/feed/",
    "theverge": "https://www.theverge.com/rss/index.xml",
    "wired": "https://www.wired.com/feed/rss",
    "arstechnica": "https://feeds.arstechnica.com/arstechnica/index",
    "mit_tech_review": "https://www.technologyreview.com/topnews.rss",
    "venturebeat": "https://venturebeat.com/feed/",
    # 개발 커뮤니티
    "hackernews": "https://news.ycombinator.com/rss",
    "reddit_ml": "https://www.reddit.com/r/MachineLearning/.rss",
    "reddit_artificial": "https://www.reddit.com/r/Artificial/.rss",
    "reddit_technology": "https://www.reddit.com/r/technology/.rss",
    # Bloomberg / Reuters (Google News RSS로 우회)
    "bloomberg_via_gnews": "https://news.google.com/rss/search?" + urlencode({
        "q": "site:bloomberg.com/technology OR site:bloomberg.com/tech",
        "hl": "en-US", "gl": "US", "ceid": "US:en"
    }),
    "reuters_tech_via_gnews": "https://news.google.com/rss/search?" + urlencode({
        "q": "site:reuters.com/technology OR site:reuters.com/technology/archive",
        "hl": "en-US", "gl": "US", "ceid": "US:en"
    }),
}

# 정규화 =============================================
def norm_item(feed_key, entry):
    title = (entry.get("title") or "").strip()
    link = (entry.get("link") or "").strip()
    summary = (entry.get("summary") or entry.get("description") or "").strip()

    # 날짜 파싱 (없으면 now)
    published = None
    for k in ("published", "updated", "created"):
        if entry.get(k):
            try:
                published = dtparse.parse(entry[k])
                break
            except Exception:
                pass
    if not published:
        published = datetime.now(timezone.utc)
    if not published.tzinfo:
        published = published.replace(tzinfo=timezone.utc)

    # 해시로 중복 제거 키
    h = hashlib.sha256(f"{title}|{link}".encode("utf-8")).hexdigest()

    return {
        "source": feed_key,
        "title": title,
        "url": link,
        "summary": summary,
        "published_at": published.isoformat(),
        "hash": h,
    }

def fetch_all(feeds: dict[str, str]):
    all_items = []
    for key, url in feeds.items():
        parsed = feedparser.parse(url)
        for e in parsed.entries:
            all_items.append(norm_item(key, e))
    return all_items

def dedup(items):
    seen = set()
    out = []
    for it in items:
        if it["hash"] in seen:
            continue
        seen.add(it["hash"])
        out.append(it)
    return out

def sort_by_date(items):
    def _dt(s):
        return dtparse.parse(s["published_at"])
    return sorted(items, key=_dt, reverse=True)



# MAIN =============================================================
if __name__ == "__main__":
    items = fetch_all(FEEDS)
    items = dedup(items)
    items = sort_by_date(items)

    # save file
    save_jsonl(itertools.islice(items, 50), "app/data/rss_data.jsonl")
