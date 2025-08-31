# app/curate_results.py
import os
import re
import json
import math
import glob
import yaml
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse

CRAWL_RESULT_DIR=os.path.join("app","crawl_result")
RESULTS_DIR = os.path.join("app", "results")
PAGES_DIR = os.path.join(CRAWL_RESULT_DIR, "pages")
CONTENTS_JSONL = os.path.join(CRAWL_RESULT_DIR, "contents.jsonl")

CURATED_JSONL = os.path.join(RESULTS_DIR, "curated.jsonl")
CURATED_MD = os.path.join(RESULTS_DIR, "curated.md")

PROFILE_YAML = os.path.join("config", "profile.yaml")
LLM_YAML = os.path.join("config", "llm.yaml")

def load_yaml(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or default

def read_jsonl(path):
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows

def read_text(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""

def safe_lower(s):
    return (s or "").lower()

def norm_domain(u):
    try:
        netloc = urlparse(u).netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc
    except Exception:
        return ""

def contains_any(text, patterns):
    text_l = safe_lower(text)
    for kw in patterns or []:
        if isinstance(kw, str) and kw.strip():
            if kw.startswith("/") and kw.endswith("/"):
                # 정규식 패턴 지원: /.../
                try:
                    if re.search(kw[1:-1], text, flags=re.IGNORECASE):
                        return True
                except re.error:
                    pass
            else:
                if kw.lower() in text_l:
                    return True
    return False

def keyword_score(text, include, exclude):
    if not text:
        return 0.0
    text_l = safe_lower(text)

    pos = 0
    for kw in include or []:
        if not kw: 
            continue
        if kw.startswith("/") and kw.endswith("/"):
            try:
                hits = re.findall(kw[1:-1], text, flags=re.IGNORECASE)
                pos += len(hits)
            except re.error:
                pass
        else:
            pos += text_l.count(kw.lower())

    neg = 0
    for kw in exclude or []:
        if not kw:
            continue
        if kw.startswith("/") and kw.endswith("/"):
            try:
                hits = re.findall(kw[1:-1], text, flags=re.IGNORECASE)
                neg += len(hits)
            except re.error:
                pass
        else:
            neg += text_l.count(kw.lower())

    # 간단 정규화: 양수는 로그 스케일, 음수는 강한 패널티
    pos_part = math.log1p(pos) if pos > 0 else 0.0
    neg_part = 1.5 * math.log1p(neg) if neg > 0 else 0.0
    return max(0.0, pos_part - neg_part)

def recency_score(fetched_at_iso, half_life_days=14, hard_days_cutoff=365):
    """
    fetched_at 기준으로 최근일수 가중.
    절반감쇠(half-life) 지수함수: score = 0.5 ** (days/half_life)
    너무 오래된 건 컷오프 페널티.
    """
    if not fetched_at_iso:
        return 0.2  # 정보 없음 → 약한 점수
    try:
        dt = datetime.fromisoformat(fetched_at_iso.replace("Z", "+00:00"))
    except Exception:
        return 0.2
    now = datetime.now(timezone.utc)
    days = max(0, (now - dt).days)
    s = 0.5 ** (days / max(1, half_life_days))
    if days > hard_days_cutoff:
        s *= 0.3
    return s

def length_score(n_chars, min_chars, max_chars):
    if n_chars is None or n_chars <= 0:
        return 0.0
    # 너무 짧으면 0, 너무 길면 완만히 감소, 적당 범위에서 최고
    if n_chars < min_chars:
        return 0.2 * (n_chars / max(1, min_chars))
    # 스위트 스팟: [min_chars, max_chars] 부근
    if n_chars <= max_chars:
        return 1.0
    # 과도하게 길면 서서히 감소
    over = n_chars - max_chars
    return max(0.4, 1.0 / (1.0 + over / 100000))

def domain_score(domain, prefer_domains, avoid_domains):
    if not domain:
        return 0.3
    if domain in (d.lower() for d in (prefer_domains or [])):
        return 1.0
    if domain in (d.lower() for d in (avoid_domains or [])):
        return 0.0
    return 0.6

def sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)

    profile = load_yaml(PROFILE_YAML, default={})
    prefs = profile.get("preferences", {})
    limits = profile.get("limits", {})
    rec = profile.get("recency", {})
    weights = profile.get("weights", {})
    snippets = profile.get("snippets", {})

    include_kw = prefs.get("include_keywords", [])
    exclude_kw = prefs.get("exclude_keywords", [])
    prefer_domains = [d.lower() for d in prefs.get("prefer_domains", [])]
    avoid_domains = [d.lower() for d in prefs.get("avoid_domains", [])]

    final_n = limits.get("final_n", 40) or 40
    min_chars = limits.get("min_chars", 500) or 500
    max_chars = limits.get("max_chars", 150000) or 150000

    use_fetched_at = rec.get("use_fetched_at", True)
    half_life_days = rec.get("half_life_days", 14) or 14
    hard_days_cutoff = rec.get("hard_days_cutoff", 365) or 365

    w_keyword = weights.get("keyword", 0.50)
    w_domain  = weights.get("domain", 0.20)
    w_recency = weights.get("recency", 0.20)
    w_length  = weights.get("length", 0.10)

    max_snip = snippets.get("max_chars", 700) or 700

    rows = read_jsonl(CONTENTS_JSONL)
    if not rows:
        print("[curate] No contents.jsonl found or empty. Run crawl step first.")
        return

    # 페이지 로드 & 스코어링
    scored = []
    for r in rows:
        if not r.get("ok"):
            continue
        url = r.get("url", "")
        title = r.get("title", "") or ""
        markdown_path = r.get("markdown_path")
        fetched_at = r.get("fetched_at") if use_fetched_at else None

        if not markdown_path:
            continue
        fullpath = markdown_path if os.path.isabs(markdown_path) else os.path.join(".", markdown_path)
        content = read_text(fullpath)
        if not content:
            continue

        # 메타 주석 제거 후 본문만으로 판단(선택)
        if content.startswith("<!--"):
            end = content.find("-->")
            if end != -1:
                content_for_score = content[end+3:].strip()
            else:
                content_for_score = content
        else:
            content_for_score = content

        n_chars = len(content_for_score)
        domain = norm_domain(url)

        ks = keyword_score(title + "\n" + content_for_score, include_kw, exclude_kw)
        ds = domain_score(domain, prefer_domains, avoid_domains)
        rs = recency_score(fetched_at, half_life_days, hard_days_cutoff)
        ls = length_score(n_chars, min_chars, max_chars)

        score = (w_keyword * ks) + (w_domain * ds) + (w_recency * rs) + (w_length * ls)

        reason_bits = []
        if ks > 0: reason_bits.append(f"keywords+:{ks:.2f}")
        if ds >= 0.9: reason_bits.append(f"prefer-domain:{domain}")
        if ds == 0.0: reason_bits.append(f"avoid-domain:{domain}")
        if rs > 0.7: reason_bits.append("very-recent")
        if n_chars < min_chars: reason_bits.append("too-short")
        if n_chars > max_chars: reason_bits.append("too-long")

        snippet = content_for_score.strip().replace("\r"," ").replace("\n", " ")
        if len(snippet) > max_snip:
            snippet = snippet[:max_snip].rstrip() + "…"

        scored.append({
            "url": url,
            "title": title,
            "domain": domain,
            "score_raw": score,
            "reasons": reason_bits,
            "fetched_at": fetched_at,
            "markdown_path": r.get("markdown_path"),
            "chars": n_chars,
            "snippet": snippet
        })

    if not scored:
        print("[curate] No scored items. Check filters or pages.")
        return

    # (옵션) LLM 재랭킹 훅: 상위 K개만 재점수 → 최종 점수에 소량 반영
    llm_cfg = load_yaml(LLM_YAML, default={})
    if llm_cfg.get("llm_enabled", False):
        top_k = min(int(llm_cfg.get("top_k_for_llm", 60) or 60), len(scored))
        # 상위 top_k만 추출
        scored.sort(key=lambda x: x["score_raw"], reverse=True)
        head = scored[:top_k]
        tail = scored[top_k:]

        # 여기에 실제 LLM 호출 코드를 붙이면 됨.
        # 예시(가짜): 내용 길이에 비례한 미세 가점
        for h in head:
            bonus = min(0.1, math.log1p(h["chars"]) / 10000.0)
            h["score_llm"] = bonus
            h["score"] = h["score_raw"] + bonus
            h["reasons"].append("llm:bonus")
        # tail은 그대로
        for t in tail:
            t["score_llm"] = 0.0
            t["score"] = t["score_raw"]
        scored = head + tail
    else:
        for it in scored:
            it["score_llm"] = 0.0
            it["score"] = it["score_raw"]

    # 최종 정렬 & 자르기
    scored.sort(key=lambda x: x["score"], reverse=True)
    final_n = int(final_n) if isinstance(final_n, int) or (isinstance(final_n, str) and final_n.isdigit()) else 40
    top = scored[:final_n]

    # 저장: JSONL
    with open(CURATED_JSONL, "w", encoding="utf-8") as f:
        for t in top:
            f.write(json.dumps(t, ensure_ascii=False) + "\n")

    # 저장: Markdown 요약
    lines = ["# Curated Results\n"]
    for i, t in enumerate(top, 1):
        lines.append(f"## {i}. {t['title'] or '(no title)'}")
        lines.append(f"- URL: {t['url']}")
        lines.append(f"- Domain: `{t['domain']}`  | Score: **{t['score']:.3f}**  | Fetched: {t.get('fetched_at') or '-'}")
        if t.get("reasons"):
            lines.append(f"- Reasons: {', '.join(t['reasons'])}")
        lines.append("")
        lines.append(t["snippet"])
        lines.append("\n---\n")

    with open(CURATED_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"[curate] wrote: {CURATED_JSONL}")
    print(f"[curate] wrote: {CURATED_MD}")
    print(f"[curate] topN={len(top)} / total={len(scored)}")

if __name__ == "__main__":
    main()
