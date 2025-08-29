# utils.py
from __future__ import annotations
from pathlib import Path
import json
from typing import Iterable, Mapping, Any

def save_jsonl(items: Iterable[Mapping[str, Any]], outpath: str | Path, limit: int | None = None) -> Path:
    """
    items: dict의 이터러블(리스트/제너레이터 모두 OK)
    outpath: 저장할 파일 경로 (예: "results/rss_data.jsonl")
    limit: 앞에서부터 최대 N개만 저장 (None이면 전부)
    return: 최종 저장된 Path
    """
    outpath = Path(outpath)
    outpath.parent.mkdir(parents=True, exist_ok=True)

    n = 0
    with outpath.open("w", encoding="utf-8") as f:
        for item in items:
            if limit is not None and n >= limit:
                break
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            n += 1
    print(f"Saved {n} items → {outpath}")
    return outpath
