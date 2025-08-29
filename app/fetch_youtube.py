#!/usr/bin/env python3
"""
YouTube 검색 유틸 (환경변수/파일변수 기반 단일 실행)

- A) 특정 채널 내 키워드 검색 (search.list with channelId + q)
- B) 전체 YouTube 키워드 검색 (search.list with q)
- 비디오 상세정보 보강 (videos.list -> duration, viewCount, likeCount 등)
- 결과: utils.save_data.save_jsonl 로 저장

사용법:
  1) .env에 YOUTUBE_API_KEY 설정
  2) 아래 CONFIG 값을 수정한 뒤
  3) python fetch_youtube.py
"""

from __future__ import annotations
import os, re, time, json
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dateutil import parser as dtparse
from dotenv import load_dotenv

# ───────────────────────────────────────────────────────────
# 0) 실행 설정 (여기만 수정해서 사용)
# ───────────────────────────────────────────────────────────
CONFIG = {
    # 'global' 또는 'channel'
    "MODE": "global",

    # 검색어
    "QUERY": "AI product launch",

    # MODE='channel'일 때만 사용 (@handle, /channel/UC..., /@handle 등 모두 허용)
    "CHANNEL": "@openai",

    # RFC3339 UTC (예: "2025-08-01T00:00:00Z") 또는 None
    "PUBLISHED_AFTER": None,

    # 가져올 최대 개수
    "LIMIT": 100,

    # 저장 경로(None이면 자동 경로: results/youtube_*.jsonl)
    "OUTPATH": "results/youtube_data.jsonl",
    # "OUTPATH": None,
}
# ───────────────────────────────────────────────────────────

# .env 로드
load_dotenv()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
BASE = "https://www.googleapis.com/youtube/v3"

# 공용 저장 유틸
from utils.save_data import save_jsonl  # ← 여기로 공통 저장 통일

# ───────────────────────────────────────────────────────────
# 유틸
# ───────────────────────────────────────────────────────────
def rfc3339(dt_str: Optional[str]) -> Optional[str]:
    if not dt_str:
        return None
    dt = dtparse.parse(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def sleep_backoff(i: int):
    time.sleep(min(2 ** i, 10))

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def extract_channel_id_from_url_or_handle(s: str) -> Optional[str]:
    s = s.strip()
    if re.fullmatch(r"UC[a-zA-Z0-9_-]{22}", s):
        return s
    m = re.search(r"/channel/(UC[a-zA-Z0-9_-]{22})", s)
    if m:
        return m.group(1)
    return None

def _slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-z0-9\-_]+", "", s)
    return s[:80] or "query"

def _default_outpath(scope: str, query: str, channel: Optional[str]) -> Path:
    q = _slug(query)
    if scope == "global":
        name = f"youtube_global_{q}.jsonl"
    else:
        ch = _slug(channel or "channel")
        name = f"youtube_channel_{ch}_{q}.jsonl"
    return Path("results") / name

# ───────────────────────────────────────────────────────────
# API 호출
# ───────────────────────────────────────────────────────────
def yt_get(client: httpx.Client, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not YOUTUBE_API_KEY:
        raise RuntimeError("환경변수 YOUTUBE_API_KEY가 설정되지 않았습니다.")
    p = {"key": YOUTUBE_API_KEY, **params}
    url = f"{BASE}/{path}"
    for attempt in range(5):
        try:
            r = client.get(url, params=p, timeout=30)
            if r.status_code == 429 or r.status_code >= 500:
                sleep_backoff(attempt)
                continue
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError:
            if attempt == 4:
                raise
            sleep_backoff(attempt)
    raise RuntimeError("YouTube API 요청 실패")

def resolve_channel_id(client: httpx.Client, ident: str) -> str:
    cid = extract_channel_id_from_url_or_handle(ident)
    if cid:
        return cid
    q = ident.lstrip("@").strip()
    data = yt_get(client, "search", {
        "part": "snippet",
        "q": q,
        "type": "channel",
        "maxResults": 1,
    })
    items = data.get("items", [])
    if not items:
        raise ValueError(f"채널을 찾을 수 없습니다: {ident}")
    return items[0]["snippet"]["channelId"]

def search_list(
    client: httpx.Client,
    q: str,
    channel_id: Optional[str] = None,
    published_after: Optional[str] = None,
    max_items: int = 50,
    order: str = "date",
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    page_token = None
    while len(results) < max_items:
        params = {
            "part": "snippet",
            "q": q,
            "type": "video",
            "order": order,
            "maxResults": min(50, max_items - len(results)),
        }
        if channel_id:
            params["channelId"] = channel_id
        if published_after:
            params["publishedAfter"] = published_after
        if page_token:
            params["pageToken"] = page_token

        data = yt_get(client, "search", params)
        for it in data.get("items", []):
            sn = it.get("snippet", {})
            results.append({
                "videoId": it["id"]["videoId"],
                "title": sn.get("title", ""),
                "description": sn.get("description", ""),
                "publishedAt": sn.get("publishedAt"),
                "channelTitle": sn.get("channelTitle"),
                "channelId": sn.get("channelId"),
                "thumbnails": sn.get("thumbnails", {}),
                "query": q,
                "scope": "channel" if channel_id else "global",
            })
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return results

def videos_list_details(client: httpx.Client, video_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    details: Dict[str, Dict[str, Any]] = {}
    for batch in chunks(video_ids, 50):
        data = yt_get(client, "videos", {
            "part": "snippet,contentDetails,statistics",
            "id": ",".join(batch),
            "maxResults": 50
        })
        for v in data.get("items", []):
            vid = v["id"]
            details[vid] = {
                "duration": v.get("contentDetails", {}).get("duration"),
                "dimension": v.get("contentDetails", {}).get("dimension"),
                "definition": v.get("contentDetails", {}).get("definition"),
                "caption": v.get("contentDetails", {}).get("caption"),
                "viewCount": v.get("statistics", {}).get("viewCount"),
                "likeCount": v.get("statistics", {}).get("likeCount"),
                "commentCount": v.get("statistics", {}).get("commentCount"),
            }
    return details

def enrich_with_details(items: List[Dict[str, Any]], details: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for it in items:
        vid = it["videoId"]
        d = details.get(vid, {})
        it2 = {**it, **d}
        it2["publishedAt"] = rfc3339(it2.get("publishedAt"))
        out.append(it2)
    return out

# ───────────────────────────────────────────────────────────
# 실행 엔트리
# ───────────────────────────────────────────────────────────
def run_channel_search(channel: str, query: str, published_after: Optional[str], limit: int) -> List[Dict[str, Any]]:
    with httpx.Client() as client:
        channel_id = resolve_channel_id(client, channel)
        items = search_list(client, q=query, channel_id=channel_id, published_after=published_after, max_items=limit)
        details = videos_list_details(client, [x["videoId"] for x in items])
        return enrich_with_details(items, details)

def run_global_search(query: str, published_after: Optional[str], limit: int) -> List[Dict[str, Any]]:
    with httpx.Client() as client:
        items = search_list(client, q=query, published_after=published_after, max_items=limit)
        details = videos_list_details(client, [x["videoId"] for x in items])
        return enrich_with_details(items, details)

def main():
    mode = CONFIG["MODE"]
    query = CONFIG["QUERY"]
    channel = CONFIG.get("CHANNEL")
    published_after = CONFIG.get("PUBLISHED_AFTER")
    limit = int(CONFIG.get("LIMIT", 50))
    outpath = CONFIG.get("OUTPATH")

    if mode not in ("global", "channel"):
        raise ValueError("CONFIG['MODE'] must be 'global' or 'channel'")

    if mode == "channel" and not channel:
        raise ValueError("CONFIG['CHANNEL'] must be set when MODE='channel'")

    if mode == "global":
        data = run_global_search(query=query, published_after=published_after, limit=limit)
        default_out = _default_outpath("global", query, None)
    else:
        data = run_channel_search(channel=channel, query=query, published_after=published_after, limit=limit)
        default_out = _default_outpath("channel", query, channel)

    out = Path(outpath) if outpath else default_out
    save_jsonl(data, out)  # ← 공통 저장

if __name__ == "__main__":
    main()
