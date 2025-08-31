# app/crawl_extract.py
import os
import json
import asyncio
import hashlib
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from crawl4ai import AsyncWebCrawler

DATA_DIR=os.path.join("app", "data")
RESULTS_DIR = os.path.join("app", "crawl_result")

INPUT_FILES = [
    os.path.join(DATA_DIR, "google_data.jsonl"),
    os.path.join(DATA_DIR, "rss_data.jsonl"),
]
OUT_JSONL = os.path.join(RESULTS_DIR, "contents.jsonl")
OUT_MD_DIR = os.path.join(RESULTS_DIR, "pages")

# ----- IO utils -----
def ensure_dirs():
    os.makedirs(DATA_DIR,exist_ok=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(OUT_MD_DIR, exist_ok=True)

def read_jsonl(path: str) -> Iterable[Dict]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                # 일부 라인이 JSON이 아닐 수 있으니 스킵
                continue

def write_jsonl(path: str, rows: List[Dict]):
    with open(path, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

# ----- Input normalization -----
def pick(d: Dict, *keys, default=None):
    for k in keys:
        if k in d and d[k]:
            return d[k]
    return default

def load_items(files: List[str]) -> List[Dict]:
    items = []
    for p in files:
        for rec in read_jsonl(p):
            url = pick(rec, "url", "link")
            title = pick(rec, "title", "htmlTitle")
            if not url:
                continue
            items.append({
                "url": url,
                "title": title or "",
                "source_file": os.path.basename(p),
            })
    # 중복 URL 제거 (유지 순서)
    seen = set()
    deduped = []
    for it in items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        deduped.append(it)
    return deduped

# ----- Crawl -----
async def fetch_one(crawler: AsyncWebCrawler, item: Dict, sem: asyncio.Semaphore, retries: int = 2, timeout: int = 30) -> Dict:
    """
    Crawl4AI로 페이지를 수집하고 결과를 표준화하여 반환.
    """
    url = item["url"]
    title = item["title"]
    out = {
        "url": url,
        "title": title,
        "source_file": item.get("source_file"),
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "ok": False,
        "error": None,
        "markdown_path": None,
        "markdown_chars": 0,
    }
    attempt = 0
    async with sem:
        while attempt <= retries:
            try:
                # Crawl4AI 0.7.x 기본 호출
                result = await crawler.arun(
                    url=url,
                    timeout=timeout,
                )
                md = (result.markdown or "").strip()
                # 빈 결과면 실패로 간주하고 재시도
                if not md:
                    raise RuntimeError("Empty markdown")
                # 저장
                fname = f"{sha1(url)}.md"
                fpath = os.path.join(OUT_MD_DIR, fname)
                with open(fpath, "w", encoding="utf-8") as f:
                    # 앞에 메타 주석 추가하면 디버깅 편함
                    meta = f"<!-- title: {title}\nurl: {url}\nfetched_at: {out['fetched_at']}\n-->".strip()
                    f.write(meta + "\n\n" + md)
                out["ok"] = True
                out["markdown_path"] = os.path.relpath(fpath, start=".").replace("\\", "/")
                out["markdown_chars"] = len(md)
                return out
            except Exception as e:
                attempt += 1
                last_err = f"{type(e).__name__}: {e}"
                if attempt > retries:
                    out["error"] = last_err
                    return out
                # 짧게 대기 후 재시도 (지수 백오프)
                await asyncio.sleep(min(2 ** attempt, 5))

# ----- Orchestrate -----
async def run_crawl(concurrency: int = 8) -> None:
    ensure_dirs()
    items = load_items(INPUT_FILES)
    if not items:
        print("[crawl] no input items found")
        return

    # 이미 수집한 URL은 건너뛰기 위해 OUT_JSONL을 읽어서 ok==True 목록을 만든다
    done_ok = set()
    if os.path.exists(OUT_JSONL):
        for rec in read_jsonl(OUT_JSONL):
            if rec.get("ok") and rec.get("url"):
                done_ok.add(rec["url"])

    pending = [it for it in items if it["url"] not in done_ok]
    if not pending:
        print("[crawl] nothing to do; all URLs already crawled.")
        return

    print(f"[crawl] total={len(items)}, already_ok={len(done_ok)}, to_crawl={len(pending)}")

    sem = asyncio.Semaphore(concurrency)
    results_batch: List[Dict] = []
    batch_size = 20  # JSONL에 주기적으로 flush

    async with AsyncWebCrawler() as crawler:
        tasks = [fetch_one(crawler, it, sem) for it in pending]
        for coro in asyncio.as_completed(tasks):
            res = await coro
            results_batch.append(res)
            # 진행상황 출력
            status = "OK" if res["ok"] else f"ERR({res['error']})"
            print(f"[{len(done_ok)+len(results_batch)}/{len(items)}] {status} → {res['url']}")
            if len(results_batch) >= batch_size:
                write_jsonl(OUT_JSONL, results_batch)
                results_batch.clear()

    # 남은 것 flush
    if results_batch:
        write_jsonl(OUT_JSONL, results_batch)

    print(f"[crawl] done. wrote results to: {OUT_JSONL}\n       markdown pages in: {OUT_MD_DIR}")

def main():
    # Windows 환경에서 콘솔용 이벤트 루프 설정은 기본으로 충분.
    asyncio.run(run_crawl(concurrency=8))

if __name__ == "__main__":
    main()
