#!/usr/bin/env python3
"""
resolve_missing_urls.py

Independent second-stage resolver for daysee_grants.xlsx.

Goal:
- ONLY process rows where official_url_status == "search_no_match"
- Use the FULL title string as the primary search key (do not split year/title)
- Restrict candidate searches to likely government domains whenever possible
- Verify candidate URLs before writing them back
- Update grants_summary / grants_detail in the main workbook
- Optionally update new_plans / updated_plans in the delta workbook
- Cache verified results to avoid re-searching the same title every week

Recommended usage in GitHub Actions after the main scraper:
    python resolve_missing_urls.py

Environment variables:
    INPUT_XLSX   (default: outputs/daysee_grants.xlsx)
    DELTA_XLSX   (default: outputs/daysee_grants_delta_only.xlsx)
    CACHE_JSON   (default: state/title_url_cache.json)
    MAX_MISSING  (default: 0 = all search_no_match rows)
"""
from __future__ import annotations

from pathlib import Path
import json
import os
import re
import time
import hashlib
from dataclasses import dataclass
from html import unescape
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote_plus, urlparse, urlunparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

INPUT_XLSX = os.getenv("INPUT_XLSX", "outputs/daysee_grants.xlsx")
DELTA_XLSX = os.getenv("DELTA_XLSX", "outputs/daysee_grants_delta_only.xlsx")
CACHE_JSON = os.getenv("CACHE_JSON", "state/title_url_cache.json")
MAX_MISSING = int(os.getenv("MAX_MISSING", "0") or "0")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}

BAD_DOMAINS = {
    "www.google.com", "google.com", "support.google.com",
    "www.bing.com", "bing.com", "duckduckgo.com", "html.duckduckgo.com",
    "facebook.com", "www.facebook.com", "instagram.com", "www.instagram.com",
    "www.104.com.tw", "104.com.tw", "youtube.com", "www.youtube.com",
}

CENTRAL_SOURCE_HINTS: Dict[str, List[str]] = {
    "勞動部": ["mol.gov.tw", "wda.gov.tw", "wlb.mol.gov.tw"],
    "教育部": ["moe.gov.tw"],
    "經濟部": ["moea.gov.tw", "sme.gov.tw", "www.sme.gov.tw"],
    "數位發展部": ["moda.gov.tw", "adi.gov.tw", "digiplus.adi.gov.tw"],
    "客家委員會": ["hakka.gov.tw"],
    "原住民族委員會": ["cip.gov.tw"],
    "文化部": ["moc.gov.tw"],
    "海洋委員會": ["oac.gov.tw"],
    "國家發展委員會": ["ndc.gov.tw"],
    "農業部": ["moa.gov.tw"],
    "環境部": ["moenv.gov.tw"],
    "衛生福利部": ["mohw.gov.tw"],
    "國科會": ["nstc.gov.tw"],
    "內政部": ["moi.gov.tw"],
    "交通部": ["motc.gov.tw", "tourism.gov.tw"],
}

LOCAL_REGION_HINTS: Dict[str, List[str]] = {
    "臺北市": ["gov.taipei", "www.gov.taipei"],
    "台北市": ["gov.taipei", "www.gov.taipei"],
    "新北市": ["ntpc.gov.tw", "www.ntpc.gov.tw", "economic.ntpc.gov.tw", "culture.ntpc.gov.tw", "sw.ntpc.gov.tw"],
    "桃園市": ["tycg.gov.tw", "www.tycg.gov.tw"],
    "臺中市": ["taichung.gov.tw", "www.taichung.gov.tw"],
    "台中市": ["taichung.gov.tw", "www.taichung.gov.tw"],
    "臺南市": ["tainan.gov.tw", "www.tainan.gov.tw"],
    "台南市": ["tainan.gov.tw", "www.tainan.gov.tw"],
    "高雄市": ["kcg.gov.tw", "www.kcg.gov.tw"],
    "基隆市": ["klcg.gov.tw", "www.klcg.gov.tw"],
    "新竹市": ["hccg.gov.tw", "www.hccg.gov.tw", "youthhsinchu.hccg.gov.tw"],
    "新竹縣": ["hsinchu.gov.tw", "www.hsinchu.gov.tw"],
    "苗栗縣": ["miaoli.gov.tw", "www.miaoli.gov.tw"],
    "彰化縣": ["changhua.gov.tw", "www.changhua.gov.tw"],
    "南投縣": ["nantou.gov.tw", "www.nantou.gov.tw"],
    "雲林縣": ["yunlin.gov.tw", "www.yunlin.gov.tw"],
    "嘉義市": ["chiayi.gov.tw", "www.chiayi.gov.tw"],
    "嘉義縣": ["cyhg.gov.tw", "www.cyhg.gov.tw"],
    "屏東縣": ["ptcg.gov.tw", "www.ptcg.gov.tw"],
    "宜蘭縣": ["yilan.gov.tw", "www.yilan.gov.tw"],
    "花蓮縣": ["hl.gov.tw", "www.hl.gov.tw"],
    "臺東縣": ["taitung.gov.tw", "www.taitung.gov.tw"],
    "台東縣": ["taitung.gov.tw", "www.taitung.gov.tw"],
    "澎湖縣": ["penghu.gov.tw", "www.penghu.gov.tw"],
    "金門縣": ["kinmen.gov.tw", "www.kinmen.gov.tw"],
    "連江縣": ["matsu.gov.tw", "www.matsu.gov.tw"],
    "馬祖": ["matsu.gov.tw", "www.matsu.gov.tw"],
}

PRIMARY_COMPARE_COLS = [
    "title", "detail_url", "plan_source", "eligible_targets", "applicable_region",
    "grant_amount", "deadline_date", "deadline_text",
    "topic_1", "topic_2", "topic_3", "topic_4", "topic_5",
    "organizer_site_url", "organizer_site_domain",
    "official_organizer_site_url", "official_organizer_domain",
    "official_url_status", "official_url_confidence",
]

@dataclass
class Candidate:
    url: str
    query: str
    scope: str
    source: str

def log(msg: str) -> None:
    print(msg, flush=True)

def normalize_text(s: Any) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unescape(s)
    s = s.replace("\u3000", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_title_key(title: str) -> str:
    s = normalize_text(title).lower()
    s = s.replace("臺", "台")
    s = re.sub(r"[｜|／/()\[\]【】「」『』：:、，,。．.；;！!？?\-_\s]+", "", s)
    return s

def sha1_short(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]

def get_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def is_google_search_url(url: str) -> bool:
    return "google.com/search?" in (url or "")

def is_probably_official_domain(domain: str) -> bool:
    domain = (domain or "").lower()
    if not domain or domain in BAD_DOMAINS:
        return False
    return (
        domain.endswith(".gov.tw")
        or domain.endswith(".gov")
        or domain in {
            "gov.taipei", "www.gov.taipei",
            "youthhsinchu.hccg.gov.tw",
        }
    )

def with_www_variants(url: str) -> List[str]:
    parsed = urlparse(url)
    host = parsed.netloc
    variants = []
    if not host:
        return [url]
    variants.append(url)
    if host.startswith("www."):
        variants.append(urlunparse(parsed._replace(netloc=host[4:])))
    else:
        variants.append(urlunparse(parsed._replace(netloc="www." + host)))
    # unique preserve order
    out = []
    seen = set()
    for v in variants:
        if v not in seen:
            out.append(v)
            seen.add(v)
    return out

def region_hint_domains(title: str, region: str) -> List[str]:
    text = f"{normalize_text(title)} {normalize_text(region)}"
    domains: List[str] = []
    for k, vals in LOCAL_REGION_HINTS.items():
        if k in text:
            domains.extend(vals)
    # unique
    out = []
    seen = set()
    for d in domains:
        if d not in seen:
            out.append(d)
            seen.add(d)
    return out

def source_hint_domains(plan_source: str) -> List[str]:
    domains: List[str] = []
    for k, vals in CENTRAL_SOURCE_HINTS.items():
        if k in normalize_text(plan_source):
            domains.extend(vals)
    out = []
    seen = set()
    for d in domains:
        if d not in seen:
            out.append(d)
            seen.add(d)
    return out

def build_scope_domains(title: str, plan_source: str, applicable_region: str) -> List[str]:
    domains = []
    domains.extend(region_hint_domains(title, applicable_region))
    domains.extend(source_hint_domains(plan_source))
    if not domains:
        # If plan_source says 縣市政府 but region is missing, try infer from title.
        domains.extend(region_hint_domains(title, title))
    # unique
    out = []
    seen = set()
    for d in domains:
        if d not in seen:
            out.append(d)
            seen.add(d)
    return out

def build_queries(full_title: str, plan_source: str, applicable_region: str) -> List[Tuple[str, str]]:
    """
    IMPORTANT:
    Per user requirement, search MUST prioritize the FULL title.
    We do not split year/title into a separate core-title search here.
    """
    full_title = normalize_text(full_title)
    plan_source = normalize_text(plan_source)
    applicable_region = normalize_text(applicable_region)
    scopes = build_scope_domains(full_title, plan_source, applicable_region)

    queries: List[Tuple[str, str]] = []
    if scopes:
        for d in scopes:
            queries.append((f'"{full_title}" site:{d}', d))
            if plan_source:
                queries.append((f'"{full_title}" "{plan_source}" site:{d}', d))
            if applicable_region and applicable_region not in {"不分縣市", "全國", "不分地區"}:
                queries.append((f'"{full_title}" "{applicable_region}" site:{d}', d))
            queries.append((f'"{full_title}" 補助 site:{d}', d))
            queries.append((f'"{full_title}" 公告 site:{d}', d))
            queries.append((f'"{full_title}" PDF site:{d}', d))
    else:
        # fallback only when no hint domain can be inferred
        queries.append((f'"{full_title}"', ""))
        if plan_source:
            queries.append((f'"{full_title}" "{plan_source}"', ""))
        if applicable_region and applicable_region not in {"不分縣市", "全國", "不分地區"}:
            queries.append((f'"{full_title}" "{applicable_region}"', ""))

    # unique preserve order
    out = []
    seen = set()
    for q, s in queries:
        key = (q, s)
        if key not in seen:
            out.append((q, s))
            seen.add(key)
    return out[:18]

def extract_bing_urls(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: List[str] = []
    for a in soup.select("li.b_algo h2 a[href], a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        domain = get_domain(href)
        if domain in BAD_DOMAINS:
            continue
        if href.startswith("http"):
            urls.append(href)
    # unique
    out = []
    seen = set()
    for u in urls:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out[:12]

def extract_ddg_urls(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: List[str] = []
    for a in soup.select("a.result__a[href], a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        domain = get_domain(href)
        if domain in BAD_DOMAINS:
            continue
        if href.startswith("http"):
            urls.append(href)
    out = []
    seen = set()
    for u in urls:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out[:12]

def search_bing(query: str) -> List[str]:
    url = "https://www.bing.com/search?q=" + quote_plus(query)
    resp = requests.get(url, headers=HEADERS, timeout=25)
    resp.raise_for_status()
    return extract_bing_urls(resp.text)

def search_ddg(query: str) -> List[str]:
    url = "https://html.duckduckgo.com/html/?q=" + quote_plus(query)
    resp = requests.get(url, headers=HEADERS, timeout=25)
    resp.raise_for_status()
    return extract_ddg_urls(resp.text)

def title_match_score(full_title: str, text: str) -> float:
    full_title = normalize_text(full_title)
    text = normalize_text(text)
    if not full_title or not text:
        return 0.0
    if full_title in text:
        return 1.0

    full_key = normalize_title_key(full_title)
    text_key = normalize_title_key(text)
    if full_key and full_key in text_key:
        return 0.95

    # token overlap, but keep full title as primary basis
    tokens = [t for t in re.split(r"\s+", full_title) if t]
    if not tokens:
        return 0.0
    hit = sum(1 for t in tokens if t in text)
    return hit / max(len(tokens), 1)

def verify_candidate(url: str, full_title: str, scope: str) -> Tuple[bool, str, float]:
    """
    Returns: (verified, final_domain, score)
    """
    best_score = 0.0
    best_domain = get_domain(url)

    for candidate_url in with_www_variants(url):
        try:
            resp = requests.get(candidate_url, headers=HEADERS, timeout=25, allow_redirects=True)
        except Exception:
            continue
        final_url = resp.url
        final_domain = get_domain(final_url)
        if final_domain in BAD_DOMAINS:
            continue
        if not is_probably_official_domain(final_domain):
            continue
        if resp.status_code >= 400:
            continue

        html = resp.text[:250000]
        soup = BeautifulSoup(html, "html.parser")

        title_tag = soup.title.get_text(" ", strip=True) if soup.title else ""
        body_text = soup.get_text(" ", strip=True)[:10000]

        score = max(
            title_match_score(full_title, title_tag),
            title_match_score(full_title, body_text),
        )

        # Strongly prefer scope-domain matches
        if scope and final_domain.endswith(scope):
            score += 0.2

        if score > best_score:
            best_score = score
            best_domain = final_domain

        if score >= 0.85:
            return True, final_domain, score

    return False, best_domain, best_score

def load_cache(path: str) -> Dict[str, Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_cache(path: str, data: Dict[str, Dict[str, Any]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def load_workbook_sheets(path: str) -> Dict[str, pd.DataFrame]:
    xls = pd.ExcelFile(path)
    return {name: pd.read_excel(path, sheet_name=name) for name in xls.sheet_names}

def patch_df(df: pd.DataFrame, cache: Dict[str, Dict[str, Any]]) -> Tuple[pd.DataFrame, Dict[str, int]]:
    if "official_url_status" not in df.columns or "title" not in df.columns:
        return df, {"patched": 0, "resolved": 0, "unchanged": 0}

    work = df.copy()
    mask = work["official_url_status"].fillna("").eq("search_no_match")
    if MAX_MISSING > 0:
        idxs = list(work.index[mask])[:MAX_MISSING]
    else:
        idxs = list(work.index[mask])

    patched = resolved = unchanged = 0

    for idx in idxs:
        title = normalize_text(work.at[idx, "title"])
        plan_source = normalize_text(work.at[idx, "plan_source"]) if "plan_source" in work.columns else ""
        region = normalize_text(work.at[idx, "applicable_region"]) if "applicable_region" in work.columns else ""

        key = sha1_short(normalize_title_key(title) + "||" + normalize_title_key(plan_source) + "||" + normalize_title_key(region))
        if key in cache:
            hit = cache[key]
            work.at[idx, "organizer_site_url"] = hit["organizer_site_url"]
            work.at[idx, "organizer_site_domain"] = hit["organizer_site_domain"]
            work.at[idx, "official_organizer_site_url"] = hit["official_organizer_site_url"]
            work.at[idx, "official_organizer_domain"] = hit["official_organizer_domain"]
            work.at[idx, "official_url_status"] = "resolver_cache_hit"
            work.at[idx, "official_url_confidence"] = hit.get("confidence", "high")
            patched += 1
            resolved += 1
            continue

        queries = build_queries(title, plan_source, region)
        candidates: List[Candidate] = []
        seen_urls = set()

        for query, scope in queries:
            urls: List[str] = []
            try:
                urls = search_bing(query)
            except Exception:
                pass
            if not urls:
                try:
                    urls = search_ddg(query)
                except Exception:
                    pass

            for u in urls:
                if u in seen_urls:
                    continue
                seen_urls.add(u)
                candidates.append(Candidate(url=u, query=query, scope=scope, source="search"))
            if len(candidates) >= 18:
                break
            time.sleep(0.2)

        best: Optional[Tuple[str, str, float, str, str]] = None
        for cand in candidates:
            ok, domain, score = verify_candidate(cand.url, title, cand.scope)
            if ok:
                if best is None or score > best[2]:
                    best = (cand.url, domain, score, cand.query, cand.scope)

        if best:
            url, domain, score, query, scope = best
            work.at[idx, "organizer_site_url"] = url
            work.at[idx, "organizer_site_domain"] = domain
            work.at[idx, "official_organizer_site_url"] = url
            work.at[idx, "official_organizer_domain"] = domain
            work.at[idx, "official_url_status"] = "resolver_verified_title_exact"
            work.at[idx, "official_url_confidence"] = "high" if score >= 0.95 else "medium"

            cache[key] = {
                "title": title,
                "plan_source": plan_source,
                "region": region,
                "organizer_site_url": url,
                "organizer_site_domain": domain,
                "official_organizer_site_url": url,
                "official_organizer_domain": domain,
                "confidence": work.at[idx, "official_url_confidence"],
                "query": query,
                "scope": scope,
            }
            patched += 1
            resolved += 1
        else:
            unchanged += 1

    return work, {"patched": patched, "resolved": resolved, "unchanged": unchanged}

def update_delta_workbook(path: str, cache: Dict[str, Dict[str, Any]]) -> Optional[str]:
    p = Path(path)
    if not p.exists():
        return None
    sheets = load_workbook_sheets(path)
    changed = False
    for sheet in ("new_plans", "updated_plans"):
        if sheet in sheets and not sheets[sheet].empty:
            patched_df, stats = patch_df(sheets[sheet], cache)
            if stats["patched"] > 0:
                sheets[sheet] = patched_df
                changed = True
    if not changed:
        return None

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=name[:31])
    return path

def main() -> None:
    input_path = Path(INPUT_XLSX)
    if not input_path.exists():
        raise FileNotFoundError(f"Input workbook not found: {INPUT_XLSX}")

    cache = load_cache(CACHE_JSON)
    sheets = load_workbook_sheets(INPUT_XLSX)

    summary_stats = {"patched": 0, "resolved": 0, "unchanged": 0}
    detail_stats = {"patched": 0, "resolved": 0, "unchanged": 0}

    if "grants_summary" in sheets:
        sheets["grants_summary"], summary_stats = patch_df(sheets["grants_summary"], cache)

    if "grants_detail" in sheets:
        sheets["grants_detail"], detail_stats = patch_df(sheets["grants_detail"], cache)

    with pd.ExcelWriter(INPUT_XLSX, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=name[:31])

    save_cache(CACHE_JSON, cache)
    delta_updated = update_delta_workbook(DELTA_XLSX, cache)

    result = {
        "input_xlsx": INPUT_XLSX,
        "delta_xlsx": DELTA_XLSX if Path(DELTA_XLSX).exists() else "",
        "summary_patched": summary_stats["patched"],
        "summary_resolved": summary_stats["resolved"],
        "summary_unchanged": summary_stats["unchanged"],
        "detail_patched": detail_stats["patched"],
        "detail_resolved": detail_stats["resolved"],
        "detail_unchanged": detail_stats["unchanged"],
        "delta_updated": bool(delta_updated),
        "cache_size": len(cache),
    }
    print(json.dumps(result, ensure_ascii=False))

if __name__ == "__main__":
    main()
