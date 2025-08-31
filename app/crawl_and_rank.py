import os, json, hashlib, math, datetime, re
from urllib.parse import urlparse, urlunparse
from typing import List, Dict, Any
from tqdm import tqdm
from dotenv import load_dotenv

# --- 1) JSONL 유틸 ---
def load_jsonl(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    yield json.loads(line)
                except:
                    continue

def write_jsonl(path: str, rows: List[Dict[str, Any]]):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

# --- 2) URL 정규화/중복 제거 ---
def canon(url: str) -> str:
    u = urlparse(url)
    return urlunparse((u.scheme, u.netloc.lower(), u.path.rstrip("/"), "", "", ""))

def unique_by_url(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = {}
    for it in items:
        if "url" not in it or "title" not in it: 
            continue
        cu = canon(it["url"])
        if cu not in seen:
            seen[cu] = {**it, "url": cu}
    return list(seen.values())

# --- 3) Crawl4AI로 본문 수집 ---
def crawl_contents(urls: List[str], concurrency: int, timeout_ms: int, render_js: bool, cache_dir: str):
    # 지연 임포트 (패키지 없으면 다른 기능만 쓰는 경우 대비)
    from crawl4ai import WebCrawler
    crawler = WebCrawler(
        concurrent_requests=concurrency,
        timeout=timeout_ms,
        render_js=render_js,
        obey_robots_txt=True,
        cache_dir=cache_dir,
    )
    # 내부에서 병렬 처리됨
    results = crawler.run(urls)
    by_url = {}
    for r in results:
        if getattr(r, "success", False) and getattr(r, "markdown", None) and len(r.markdown) > 0:
            by_url[r.url] = r.markdown
    return by_url

# --- 4) 간단 규칙 점수 ---
KEYWEIGHTS = {
    # 너 취향 키워드 예시 (원하면 수정/추가)
    "computer vision": 1.5, "rag": 1.2, "agv": 1.4, "unity": 1.1,
    "pose estimation": 1.6, "langchain": 1.1, "mcp": 1.3,
    "retrieval": 1.1, "pgvector": 1.0, "weaviate": 1.0,
    "fastapi": 1.0, "sqlalchemy": 0.9, "warehouse": 1.1
}

def recency_boost(published_at: str, half_life_days: int = 30) -> float:
    if not published_at:
        return 0.0
    try:
        d = datetime.datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    except:
        return 0.0
    days = (datetime.datetime.utcnow() - d).days
    return math.exp(-math.log(2) * days / half_life_days)  # 0~1

def score_rules(title: str, content: str, published_at: str) -> float:
    title_l = (title or "").lower()
    content_l = (content or "").lower()
    base = 0.0
    for k, w in KEYWEIGHTS.items():
        kL = k.lower()
        hit = (kL in content_l) + (kL in title_l) * 2  # 타이틀 적중 가중
        base += w * hit
    exact = 1.0 if any(q in title_l for q in ["tutorial","guide","paper","benchmark","공식","심층"]) else 0.0
    rec = recency_boost(published_at)
    length_ok = 1.0 if content and len(content) >= int(os.getenv("MIN_CONTENT_CHARS", "800")) else 0.0
    return 0.55*base + 0.20*exact + 0.15*rec + 0.10*length_ok

# --- 5) 임베딩 점수 (유사도) ---
_model_cache = None
def get_embed_model():
    global _model_cache
    if _model_cache is None:
        from sentence_transformers import SentenceTransformer
        # 한글 성능 양호한 멀티링궐 e5
        _model_cache = SentenceTransformer("intfloat/multilingual-e5-base")
    return _model_cache

def emb_cosine(a, b):
    import numpy as np
    return float(np.dot(a, b))

def embed_text(text: str):
    m = get_embed_model()
    v = m.encode([text], normalize_embeddings=True)[0]
    return v

def score_embed(profile_text: str, title: str, content: str) -> float:
    if not content:
        return -1.0
    # 너무 긴 본문은 앞부분만 사용 (속도/메모리 절약)
    text = (title or "") + "\n" + (content or "")[:4000]
    v_doc = embed_text(text)
    v_profile = embed_text(profile_text)
    return emb_cosine(v_profile, v_doc)

# --- 6) 스코어 융합 ---
def fuse_score(rule_s: float, emb_s: float) -> float:
    rule_s = max(0.0, rule_s if rule_s is not None else 0.0)
    emb_s  = max(0.0, emb_s  if emb_s  is not None else 0.0)
    return 0.5*rule_s + 0.5*emb_s

# --- 7) 메인 파이프라인 ---
def main():
    load_dotenv()
    PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
    results_dir = os.path.join(PROJECT_ROOT, "results")

    rss_path    = os.path.join(results_dir, "rss_data.jsonl")
    google_path = os.path.join(results_dir, "google_data.jsonl")

    inputs = []
    if os.path.exists(rss_path):
        inputs.extend(list(load_jsonl(rss_path)))
    if os.path.exists(google_path):
        inputs.extend(list(load_jsonl(google_path)))

    # 공통 스키마 & 중복 제거
    items = []
    for j in inputs:
        if "url" in j and "title" in j:
            items.append({
                "id": j.get("id") or hashlib.md5(j["url"].encode()).hexdigest(),
                "source": j.get("source") or ("rss" if "rss" in rss_path else "google"),
                "title": j["title"].strip(),
                "url": j["url"].strip(),
                "published_at": j.get("published_at")
            })
    items = unique_by_url(items)

    if not items:
        print("No items found in rss/google jsonl.")
        return

    # 크롤 설정
    concurrency = int(os.getenv("CRAWL_CONCURRENCY", "12"))
    timeout_ms  = int(os.getenv("CRAWL_TIMEOUT_MS", "30000"))
    render_js   = os.getenv("CRAWL_RENDER_JS", "true").lower() == "true"
    cache_dir   = os.getenv("CRAWL_CACHE_DIR", ".c4ai_cache")

    # URLs 크롤링
    urls = [it["url"] for it in items]
    print(f"[crawl] {len(urls)} urls, concurrency={concurrency}, js={render_js}")
    contents_by_url = crawl_contents(urls, concurrency, timeout_ms, render_js, cache_dir)

    # 점수 계산
    profile_text = os.getenv("PROFILE_TEXT", "나는 Computer Vision, RAG, AGV, Unity, Pose Estimation 관련 글을 좋아한다.")
    curated = []
    for it in tqdm(items, desc="Scoring"):
        content = contents_by_url.get(it["url"])
        if not content:
            continue
        s_rule = score_rules(it["title"], content, it.get("published_at"))
        s_emb  = score_embed(profile_text, it["title"], content)
        s_final = fuse_score(s_rule, s_emb)
        curated.append({
            "id": it["id"],
            "title": it["title"],
            "url": it["url"],
            "published_at": it.get("published_at"),
            "score_rule": round(s_rule, 4),
            "score_embed": round(s_emb, 4),
            "score": round(s_final, 4),
            "reason": {
                "profile": profile_text[:120] + ("..." if len(profile_text) > 120 else "")
            }
        })

    # 정렬 & 상위 N
    curated.sort(key=lambda x: x["score"], reverse=True)
    final_n = os.getenv("FINAL_N")
    try:
        final_n = int(final_n) if final_n is not None else 40
    except:
        final_n = 40  # NoneType/변환 오류 방지 (네가 겪던 에러 차단)

    out = curated[:final_n]
    out_path = os.path.join(results_dir, "curated.jsonl")
    write_jsonl(out_path, out)

    print(f"[done] kept={len(out)}/{len(curated)} -> {out_path}")
    # 디버깅용 상위 5개 출력
    for i, row in enumerate(out[:5], start=1):
        print(f"{i:>2}. {row['score']:.3f}  {row['title']}  ({row['url']})")

if __name__ == "__main__":
    main()
