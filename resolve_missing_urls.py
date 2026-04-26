#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import time
import hashlib
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote_plus, urlparse, urlunparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

INPUT_XLSX = os.getenv("INPUT_XLSX", "outputs/daysee_grants.xlsx")
DELTA_XLSX = os.getenv("DELTA_XLSX", "outputs/daysee_grants_delta_only.xlsx")
CACHE_JSON = os.getenv("CACHE_JSON", "state/title_url_cache.json")
MAX_MISSING = int(os.getenv("MAX_MISSING", "0") or "0")  # 0 = all rows
MAX_RUNTIME_SECONDS = int(os.getenv("MAX_RUNTIME_SECONDS", "2400") or "2400")  # default 40 min
VERIFY_TIMEOUT = int(os.getenv("VERIFY_TIMEOUT", "12") or "12")
SEARCH_TIMEOUT = int(os.getenv("SEARCH_TIMEOUT", "18") or "18")

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
    "www.dayseechat.com", "dayseechat.com",
}

CENTRAL_SOURCE_HINTS: Dict[str, List[str]] = {
    "勞動部": ["mol.gov.tw", "wlb.mol.gov.tw", "wda.gov.tw"],
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

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

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
    s = normalize_text(title).lower().replace("臺", "台")
    s = re.sub(r"[｜|／/()\[\]【】「」『』：:、，,。．.；;！!？?\-_\s]+", "", s)
    return s

def cache_key(title: str, source: str, region: str) -> str:
    raw = "||".join([normalize_title_key(title), normalize_title_key(source), normalize_title_key(region)])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

def get_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def is_probably_official_domain(domain: str) -> bool:
    domain = (domain or "").lower()
    if not domain or domain in BAD_DOMAINS:
        return False
    return (
        domain.endswith(".gov.tw")
        or domain.endswith(".gov")
        or domain in {"gov.taipei", "www.gov.taipei", "youthhsinchu.hccg.gov.tw"}
    )

def with_www_variants(url: str) -> List[str]:
    parsed = urlparse(url)
    host = parsed.netloc
    if not host:
        return [url]
    variants = [url]
    if host.startswith("www."):
        variants.append(urlunparse(parsed._replace(netloc=host[4:])))
    else:
        variants.append(urlunparse(parsed._replace(netloc="www." + host)))
    out, seen = [], set()
    for v in variants:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out

def region_hint_domains(title: str, region: str) -> List[str]:
    text = f"{normalize_text(title)} {normalize_text(region)}"
    domains: List[str] = []
    for k, vals in LOCAL_REGION_HINTS.items():
        if k in text:
            domains.extend(vals)
    out, seen = [], set()
    for d in domains:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out

def source_hint_domains(plan_source: str) -> List[str]:
    domains: List[str] = []
    for k, vals in CENTRAL_SOURCE_HINTS.items():
        if k in normalize_text(plan_source):
            domains.extend(vals)
    out, seen = [], set()
    for d in domains:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out

def build_scope_domains(title: str, plan_source: str, region: str) -> List[str]:
    domains: List[str] = []
    domains.extend(region_hint_domains(title, region))
    domains.extend(source_hint_domains(plan_source))
    if not domains:
        domains.extend(region_hint_domains(title, title))
    out, seen = [], set()
    for d in domains:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out[:5]

def build_queries(full_title: str, plan_source: str, region: str) -> List[Tuple[str, str]]:
    """
    Follow user requirement: use FULL title, do NOT split year/title.
    Keep query count intentionally small for runtime stability.
    """
    full_title = normalize_text(full_title)
    plan_source = normalize_text(plan_source)
    region = normalize_text(region)
    scopes = build_scope_domains(full_title, plan_source, region)

    queries: List[Tuple[str, str]] = []
    if scopes:
        for d in scopes[:3]:
            queries.append((f'"{full_title}" site:{d}', d))
            if plan_source:
                queries.append((f'"{full_title}" "{plan_source}" site:{d}', d))
            if region and region not in {"不分縣市", "全國", "不分地區"}:
                queries.append((f'"{full_title}" "{region}" site:{d}', d))
    else:
        queries.append((f'"{full_title}"', ""))
        if plan_source:
            queries.append((f'"{full_title}" "{plan_source}"', ""))
        if region and region not in {"不分縣市", "全國", "不分地區"}:
            queries.append((f'"{full_title}" "{region}"', ""))

    out, seen = [], set()
    for q, s in queries:
        key = (q, s)
        if key not in seen:
            seen.add(key)
            out.append((q, s))
    return out[:6]

def extract_bing_urls(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: List[str] = []
    for a in soup.select("li.b_algo h2 a[href], a[href]"):
        href = a.get("href", "").strip()
        if not href.startswith("http"):
            continue
        domain = get_domain(href)
        if domain in BAD_DOMAINS:
            continue
        urls.append(href)
    out, seen = [], set()
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out[:6]

def extract_ddg_urls(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: List[str] = []
    for a in soup.select("a.result__a[href], a[href]"):
        href = a.get("href", "").strip()
        if not href.startswith("http"):
            continue
        domain = get_domain(href)
        if domain in BAD_DOMAINS:
            continue
        urls.append(href)
    out, seen = [], set()
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out[:6]

def search_bing(query: str) -> List[str]:
    url = "https://www.bing.com/search?q=" + quote_plus(query)
    resp = SESSION.get(url, timeout=SEARCH_TIMEOUT)
    resp.raise_for_status()
    return extract_bing_urls(resp.text)

def search_ddg(query: str) -> List[str]:
    url = "https://html.duckduckgo.com/html/?q=" + quote_plus(query)
    resp = SESSION.get(url, timeout=SEARCH_TIMEOUT)
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
        return 0.96
    return 0.0

def verify_candidate(url: str, full_title: str, scope: str) -> Tuple[bool, str, float]:
    best_score = 0.0
    best_domain = get_domain(url)

    for candidate_url in with_www_variants(url):
        try:
            resp = SESSION.get(candidate_url, timeout=VERIFY_TIMEOUT, allow_redirects=True)
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

        html = resp.text[:200000]
        soup = BeautifulSoup(html, "html.parser")
        title_tag = soup.title.get_text(" ", strip=True) if soup.title else ""
        body_text = soup.get_text(" ", strip=True)[:8000]

        score = max(
            title_match_score(full_title, title_tag),
            title_match_score(full_title, body_text),
        )
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

def patch_df(df: pd.DataFrame, cache: Dict[str, Dict[str, Any]], time_start: float) -> Tuple[pd.DataFrame, Dict[str, int], bool]:
    work = df.copy()
    if "official_url_status" not in work.columns or "title" not in work.columns:
        return work, {"patched": 0, "resolved": 0, "unchanged": 0, "cache_hit": 0}, False

    mask = work["official_url_status"].fillna("").eq("search_no_match")
    idxs = list(work.index[mask])
    if MAX_MISSING > 0:
        idxs = idxs[:MAX_MISSING]

    stats = {"patched": 0, "resolved": 0, "unchanged": 0, "cache_hit": 0}
    timed_out = False

    for n, idx in enumerate(idxs, start=1):
        if time.time() - time_start > MAX_RUNTIME_SECONDS:
            timed_out = True
            log(f"[resolver] Stop early due to runtime budget after {n-1} rows.")
            break

        title = normalize_text(work.at[idx, "title"])
        plan_source = normalize_text(work.at[idx, "plan_source"]) if "plan_source" in work.columns else ""
        region = normalize_text(work.at[idx, "applicable_region"]) if "applicable_region" in work.columns else ""
        key = cache_key(title, plan_source, region)

        if key in cache:
            hit = cache[key]
            work.at[idx, "organizer_site_url"] = hit["organizer_site_url"]
            work.at[idx, "organizer_site_domain"] = hit["organizer_site_domain"]
            work.at[idx, "official_organizer_site_url"] = hit["official_organizer_site_url"]
            work.at[idx, "official_organizer_domain"] = hit["official_organizer_domain"]
            work.at[idx, "official_url_status"] = "resolver_cache_hit"
            work.at[idx, "official_url_confidence"] = hit.get("confidence", "high")
            stats["patched"] += 1
            stats["resolved"] += 1
            stats["cache_hit"] += 1
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

            for u in urls[:4]:
                if u in seen_urls:
                    continue
                seen_urls.add(u)
                candidates.append(Candidate(url=u, query=query, scope=scope, source="search"))

            # runtime-friendly: stop early if we already have enough candidates
            if len(candidates) >= 6:
                break
            time.sleep(0.15)

        best: Optional[Tuple[str, str, float, str, str]] = None
        for cand in candidates[:6]:
            ok, domain, score = verify_candidate(cand.url, title, cand.scope)
            if ok and (best is None or score > best[2]):
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
            stats["patched"] += 1
            stats["resolved"] += 1
        else:
            stats["unchanged"] += 1

    return work, stats, timed_out

def update_delta_workbook(path: str, cache: Dict[str, Dict[str, Any]], time_start: float) -> Tuple[bool, bool]:
    p = Path(path)
    if not p.exists():
        return False, False
    sheets = load_workbook_sheets(path)
    changed = False
    timed_out = False
    for sheet in ("new_plans", "updated_plans"):
        if sheet in sheets and not sheets[sheet].empty:
            patched_df, stats, t = patch_df(sheets[sheet], cache, time_start)
            timed_out = timed_out or t
            if stats["patched"] > 0:
                sheets[sheet] = patched_df
                changed = True
            if timed_out:
                break
    if changed:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for name, df in sheets.items():
                df.to_excel(writer, index=False, sheet_name=name[:31])
    return changed, timed_out

def main() -> None:
    started = time.time()
    input_path = Path(INPUT_XLSX)
    if not input_path.exists():
        raise FileNotFoundError(f"Input workbook not found: {INPUT_XLSX}")

    cache = load_cache(CACHE_JSON)
    sheets = load_workbook_sheets(INPUT_XLSX)

    summary_stats = {"patched": 0, "resolved": 0, "unchanged": 0, "cache_hit": 0}
    detail_stats = {"patched": 0, "resolved": 0, "unchanged": 0, "cache_hit": 0}
    timed_out = False

    if "grants_summary" in sheets:
        sheets["grants_summary"], summary_stats, timed_out = patch_df(sheets["grants_summary"], cache, started)

    if "grants_detail" in sheets and not timed_out:
        sheets["grants_detail"], detail_stats, timed_out = patch_df(sheets["grants_detail"], cache, started)

    with pd.ExcelWriter(INPUT_XLSX, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=name[:31])

    save_cache(CACHE_JSON, cache)
    delta_updated, delta_timed_out = update_delta_workbook(DELTA_XLSX, cache, started)
    timed_out = timed_out or delta_timed_out

    result = {
        "input_xlsx": INPUT_XLSX,
        "delta_xlsx": DELTA_XLSX if Path(DELTA_XLSX).exists() else "",
        "summary_patched": summary_stats["patched"],
        "summary_resolved": summary_stats["resolved"],
        "summary_unchanged": summary_stats["unchanged"],
        "summary_cache_hit": summary_stats["cache_hit"],
        "detail_patched": detail_stats["patched"],
        "detail_resolved": detail_stats["resolved"],
        "detail_unchanged": detail_stats["unchanged"],
        "detail_cache_hit": detail_stats["cache_hit"],
        "delta_updated": bool(delta_updated),
        "cache_size": len(cache),
        "timed_out_early": timed_out,
        "elapsed_seconds": round(time.time() - started, 1),
    }
    print(json.dumps(result, ensure_ascii=False))

if __name__ == "__main__":
    main()
