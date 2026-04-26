#!/usr/bin/env python3
"""
Daysee grant scraper with title-driven official URL resolution.

Design goals
------------
1. Stable crawl of the Daysee listing and detail pages.
2. Treat `title` as the primary key for URL resolution.
3. Resolve organizer/official URLs using:
   - verified direct links from the page when available
   - government-domain-constrained search (Bing HTML, optional DDG fallback)
   - local cache of previously verified title -> URL mappings
4. Never invent government domains by string concatenation.
5. Verify candidate URLs before writing them into Excel.
6. Produce both a full workbook and a delta workbook.

Environment variables
---------------------
OUTPUT_XLSX   default: outputs/daysee_grants.xlsx
DELTA_XLSX    default: outputs/daysee_grants_delta_only.xlsx
PREVIOUS_XLSX default: state/daysee_grants_latest.xlsx
CACHE_JSON    default: state/title_url_cache.json
MAX_PAGES     default: 25
REQUEST_TIMEOUT default: 20

Notes
-----
- This script avoids paid APIs.
- Search is best-effort. For future new plans, verification + cache make results
  more stable than scraping raw Google search URLs, but there can still be edge cases.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, quote_plus, unquote_plus, urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from pydantic import BaseModel, Field, HttpUrl, field_validator

# -------------------------
# Configuration
# -------------------------
BASE_URL = "https://dayseechat.com"
LISTING_URL = f"{BASE_URL}/explore-grant/"
OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", "outputs/daysee_grants.xlsx")
DELTA_XLSX = os.getenv("DELTA_XLSX", "outputs/daysee_grants_delta_only.xlsx")
PREVIOUS_XLSX = os.getenv("PREVIOUS_XLSX", "state/daysee_grants_latest.xlsx")
CACHE_JSON = os.getenv("CACHE_JSON", "state/title_url_cache.json")
MAX_PAGES = int(os.getenv("MAX_PAGES", "25"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))

Path(OUTPUT_XLSX).parent.mkdir(parents=True, exist_ok=True)
Path(DELTA_XLSX).parent.mkdir(parents=True, exist_ok=True)
Path(PREVIOUS_XLSX).parent.mkdir(parents=True, exist_ok=True)
Path(CACHE_JSON).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
SEARCH_EXCLUDE_DOMAINS = {
    "google.com",
    "www.google.com",
    "support.google.com",
    "bing.com",
    "www.bing.com",
    "duckduckgo.com",
    "html.duckduckgo.com",
    "facebook.com",
    "www.facebook.com",
    "instagram.com",
    "www.instagram.com",
    "youtube.com",
    "www.youtube.com",
    "104.com.tw",
    "www.104.com.tw",
    "linkedin.com",
    "www.linkedin.com",
    "x.com",
    "twitter.com",
}

CENTRAL_SOURCE_HINTS: Dict[str, List[str]] = {
    "勞動部": ["www.mol.gov.tw", "wlb.mol.gov.tw", "wda.gov.tw"],
    "教育部": ["www.edu.tw", "depart.moe.edu.tw", "www.moe.gov.tw"],
    "經濟部": ["www.moea.gov.tw", "gcis.nat.gov.tw", "www.sme.gov.tw"],
    "數位發展部": ["www.moda.gov.tw", "moda.gov.tw", "digiplus.adi.gov.tw"],
    "客家委員會": ["www.hakka.gov.tw"],
    "原住民族委員會": ["www.cip.gov.tw"],
    "海洋委員會": ["www.oac.gov.tw", "oac.gov.tw"],
    "國家發展委員會": ["www.ndc.gov.tw", "ndc.gov.tw"],
    "文化部": ["www.moc.gov.tw", "grants.moc.gov.tw", "mocfile.moc.gov.tw"],
    "衛生福利部": ["www.mohw.gov.tw", "www.mohw.gov.tw/mp-1.html"],
    "農業部": ["www.moa.gov.tw", "agronet.zero.moa.gov.tw"],
    "環境部": ["www.moenv.gov.tw", "www.epa.gov.tw"],
    "交通部": ["www.motc.gov.tw", "www.taiwan.net.tw"],
}

LOCAL_GOV_HINTS: Dict[str, List[str]] = {
    "臺北": ["www.gov.taipei"],
    "台北": ["www.gov.taipei"],
    "新北": ["www.ntpc.gov.tw", "www.economic.ntpc.gov.tw", "www.culture.ntpc.gov.tw", "www.sw.ntpc.gov.tw"],
    "桃園": ["www.tycg.gov.tw", "youth.tycg.gov.tw"],
    "臺中": ["www.taichung.gov.tw", "www.economic.taichung.gov.tw", "www.culture.taichung.gov.tw"],
    "台中": ["www.taichung.gov.tw", "www.economic.taichung.gov.tw", "www.culture.taichung.gov.tw"],
    "臺南": ["www.tainan.gov.tw", "economic.tainan.gov.tw", "culture.tainan.gov.tw"],
    "台南": ["www.tainan.gov.tw", "economic.tainan.gov.tw", "culture.tainan.gov.tw"],
    "高雄": ["www.kcg.gov.tw", "edbkcg.kcg.gov.tw", "khh.travel", "youth.kcg.gov.tw"],
    "基隆": ["www.klcg.gov.tw"],
    "新竹市": ["www.hccg.gov.tw", "youthhsinchu.hccg.gov.tw"],
    "新竹縣": ["www.hsinchu.gov.tw"],
    "苗栗": ["www.miaoli.gov.tw"],
    "彰化": ["www.chcg.gov.tw", "www.changhua.gov.tw"],
    "南投": ["www.nantou.gov.tw"],
    "雲林": ["www.yunlin.gov.tw"],
    "嘉義市": ["www.chiayi.gov.tw"],
    "嘉義縣": ["www.cyhg.gov.tw"],
    "屏東": ["www.pthg.gov.tw"],
    "宜蘭": ["www.e-land.gov.tw"],
    "花蓮": ["www.hl.gov.tw"],
    "臺東": ["www.taitung.gov.tw"],
    "台東": ["www.taitung.gov.tw"],
    "澎湖": ["www.penghu.gov.tw"],
    "金門": ["www.kinmen.gov.tw"],
    "連江": ["www.matsu.gov.tw"],
    "馬祖": ["www.matsu.gov.tw"],
}

# -------------------------
# Models
# -------------------------
class ListingItem(BaseModel):
    title: str
    detail_url: HttpUrl
    plan_source: str = ""
    eligible_targets: str = ""
    applicable_region: str = ""
    grant_amount: str = ""
    deadline_date: str = ""
    deadline_text: str = ""
    topic_1: str = ""
    topic_2: str = ""
    topic_3: str = ""
    topic_4: str = ""
    topic_5: str = ""


class DetailItem(ListingItem):
    organizer_site_url_raw: str = ""
    organizer_search_query: str = ""
    organizer_search_scope: str = ""
    organizer_site_url: str = ""
    organizer_site_domain: str = ""
    official_organizer_site_url: str = ""
    official_organizer_domain: str = ""
    official_url_status: str = ""
    official_url_confidence: str = ""
    plan_background: str = ""
    key_point_1: str = ""
    key_point_2: str = ""
    key_point_3: str = ""
    key_point_4: str = ""
    key_point_5: str = ""
    application_tips: str = ""
    organizer_site_note: str = ""
    raw_text: str = ""

    @field_validator("detail_url", mode="before")
    @classmethod
    def ensure_detail_url(cls, value: Any) -> Any:
        return str(value)


@dataclass
class CandidateURL:
    url: str
    source: str
    scope: str
    query: str
    score: float = 0.0


# -------------------------
# Utility helpers
# -------------------------
def clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    return text


def split_lines(text: str) -> List[str]:
    return [clean_text(x) for x in re.split(r"[\r\n]+", text) if clean_text(x)]




def normalize_label_line(line: str) -> str:
    line = clean_text(line)
    line = line.lstrip("*•·- ").strip()
    line = re.sub(r"\s*[:：]\s*", "：", line)
    return line
def get_domain(url: str) -> str:
    if not url:
        return ""
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def is_google_search_url(url: str) -> bool:
    d = get_domain(url)
    return "google." in d and "/search" in url


def extract_google_query(url: str) -> str:
    try:
        qs = parse_qs(urlparse(url).query)
        return unquote_plus(qs.get("q", [""])[0]).strip()
    except Exception:
        return ""


def is_probably_government_domain(domain: str) -> bool:
    d = domain.lower().strip()
    if not d:
        return False
    gov_markers = [".gov.tw", ".gov", "gov.taipei", "nat.gov.tw", "kcg.gov.tw", "hccg.gov.tw"]
    return any(m in d for m in gov_markers)


def normalize_title(title: str) -> Dict[str, str]:
    full = clean_text(title)
    core = full
    # remove leading year markers
    core = re.sub(r"^(\d{3,4}|20\d{2})年度?", "", core)
    core = re.sub(r"^(\d{3,4}|20\d{2})年", "", core)
    core = re.sub(r"^[（(【\[]?\d{3,4}[）)】\]]", "", core)
    core = re.sub(r"\b(徵件公告|補助要點|申請須知|計畫書|作業要點)$", "", core)
    core = clean_text(core)
    keywords = re.sub(r"[｜|、，,。．()（）【】\[\]：:]+", " ", core)
    keywords = re.sub(r"\s+", " ", keywords).strip()
    return {"full": full, "core": core or full, "keywords": keywords or full}


def title_key(title: str, source: str, region: str) -> str:
    norm = normalize_title(title)
    raw = f"{norm['core']}||{clean_text(source)}||{clean_text(region)}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def keyword_overlap_score(title: str, text: str) -> float:
    title_kw = {x for x in normalize_title(title)["keywords"].split(" ") if len(x) >= 2}
    body_kw = set(re.findall(r"[\u4e00-\u9fffA-Za-z0-9]{2,}", text))
    if not title_kw:
        return 0.0
    inter = len(title_kw & body_kw)
    return inter / max(len(title_kw), 1)


def safe_request(method: str, url: str, **kwargs) -> Optional[requests.Response]:
    headers = kwargs.pop("headers", {}) or {}
    headers.setdefault("User-Agent", UA)
    headers.setdefault("Accept-Language", "zh-TW,zh;q=0.9,en;q=0.8")
    try:
        resp = requests.request(method, url, timeout=REQUEST_TIMEOUT, headers=headers, allow_redirects=True, **kwargs)
        return resp
    except Exception:
        return None


# -------------------------
# Search cache
# -------------------------
def load_cache() -> Dict[str, Dict[str, Any]]:
    path = Path(CACHE_JSON)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    Path(CACHE_JSON).write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


# -------------------------
# Daysee parsing
# -------------------------

def extract_after_label(lines: Sequence[str], start_label: str, stop_labels: Sequence[str]) -> List[str]:
    capture = False
    out: List[str] = []
    for raw_line in lines:
        line = normalize_label_line(raw_line)
        if line.startswith(start_label):
            capture = True
            tail = clean_text(line.replace(start_label, "", 1))
            if tail:
                out.append(tail)
            continue
        if capture and any(line.startswith(x) for x in stop_labels):
            break
        if capture and line:
            if "你可能也會喜歡" in line or line.startswith("補助金額："):
                break
            out.append(line)
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq


def parse_total_results(html: str) -> int:
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    m = re.search(r"(\d+)\s*out\s*of\s*(\d+)\s*results", text, re.I)
    if m:
        return int(m.group(2))
    return 0



def parse_listing_page(html: str, seen_urls: set[str]) -> List[ListingItem]:
    soup = BeautifulSoup(html, "html.parser")
    lines = split_lines(soup.get_text("\n", strip=True))

    # title links in visible order
    title_links: List[Tuple[str, str]] = []
    for a in soup.select('a[href*="/subsidy/grant-"]'):
        href = urljoin(BASE_URL, a.get("href", "").strip())
        title = clean_text(a.get_text(" ", strip=True))
        if href and title:
            title_links.append((title, href))

    results: List[ListingItem] = []
    cursor = 0
    for idx, (title, href) in enumerate(title_links):
        if not href or href in seen_urls:
            continue

        title_idx = -1
        for i in range(cursor, len(lines)):
            if clean_text(lines[i]) == title:
                title_idx = i
                break
        if title_idx == -1:
            continue

        next_title_idx = len(lines)
        if idx + 1 < len(title_links):
            next_title = title_links[idx + 1][0]
            for j in range(title_idx + 1, len(lines)):
                if clean_text(lines[j]) == next_title:
                    next_title_idx = j
                    break

        card_lines = lines[title_idx:next_title_idx]
        prev_lines = lines[max(0, title_idx - 5):title_idx]

        grant_amount = ""
        for prev in reversed(prev_lines):
            prev = normalize_label_line(prev)
            if prev.startswith("補助金額："):
                grant_amount = clean_text(prev.replace("補助金額：", "", 1))
                break

        topics = extract_after_label(card_lines, "＃關注議題：", ["＃補助對象：", "＃計畫來源：", "＃補助金額：", "截止日期："])
        recipients = extract_after_label(card_lines, "＃補助對象：", ["＃計畫來源：", "＃補助金額：", "截止日期："])
        sources = extract_after_label(card_lines, "＃計畫來源：", ["＃補助金額：", "截止日期："])

        deadline_date = ""
        for line in card_lines:
            line_n = normalize_label_line(line)
            if line_n.startswith("截止日期："):
                m = re.search(r"(\d{4}-\d{2}-\d{2})", line_n)
                if m:
                    deadline_date = m.group(1)
                break

        item = ListingItem(
            title=title,
            detail_url=href,
            plan_source="｜".join(sources),
            eligible_targets="｜".join(recipients),
            applicable_region="",
            grant_amount=grant_amount,
            deadline_date=deadline_date,
            deadline_text="",
            topic_1=topics[0] if len(topics) > 0 else "",
            topic_2=topics[1] if len(topics) > 1 else "",
            topic_3=topics[2] if len(topics) > 2 else "",
            topic_4=topics[3] if len(topics) > 3 else "",
            topic_5=topics[4] if len(topics) > 4 else "",
        )
        results.append(item)
        seen_urls.add(href)
        cursor = title_idx + 1

    return results


def find_google_search_link(soup: BeautifulSoup) -> str:
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if is_google_search_url(href):
            return href
    return ""



def parse_detail_page(html: str, detail_url: str) -> DetailItem:
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)
    lines = split_lines(page_text)

    title = ""
    for sel in ["h1", "h2", "title"]:
        node = soup.select_one(sel)
        if node and clean_text(node.get_text(" ", strip=True)):
            title = clean_text(node.get_text(" ", strip=True))
            break
    if title.endswith("｜小社區大事件"):
        title = clean_text(title.replace("｜小社區大事件", ""))

    trunc_idx = len(lines)
    for i, line in enumerate(lines):
        if "你可能也會喜歡這些資訊" in line or "返回主頁" in line:
            trunc_idx = i
            break
    core_lines = lines[:trunc_idx]

    def after(label: str, stops: Sequence[str]) -> List[str]:
        return extract_after_label(core_lines, label, stops)

    plan_source = "｜".join(after("計畫來源：", ["補助對象：", "適用地區：", "補助金額：", "截止日期：", "關注議題："]))
    eligible_targets = "｜".join(after("補助對象：", ["適用地區：", "補助金額：", "截止日期：", "關注議題："]))
    applicable_region = "｜".join(after("適用地區：", ["補助金額：", "截止日期：", "關注議題："]))
    amount_values = after("補助金額：", ["截止日期：", "關注議題：", "申請文件：", "計畫背景：", "計畫重點：", "撰寫技巧："])
    grant_amount = "｜".join([
        x for x in amount_values
        if x and len(x) < 40 and "計畫" not in x and "關注議題" not in x and "補助對象" not in x and not x.isdigit()
    ])

    deadline_date = ""
    deadline_text = ""
    for line in core_lines:
        line_n = normalize_label_line(line)
        if line_n.startswith("截止日期："):
            payload = clean_text(line_n.replace("截止日期：", "", 1))
            m = re.search(r"(\d{4}-\d{2}-\d{2})", payload)
            if m:
                deadline_date = m.group(1)
                rest = payload.replace(deadline_date, "").strip(" ｜|")
                deadline_text = rest
            else:
                deadline_text = payload
            break

    topics = after("關注議題：", ["申請文件：", "計畫背景：", "計畫重點：", "撰寫技巧："])
    plan_background = "\n".join(after("計畫背景：", ["計畫重點：", "撰寫技巧：", "申請文件："]))
    points = after("計畫重點：", ["撰寫技巧：", "申請文件："])
    tips = "\n".join(after("撰寫技巧：", ["申請文件："]))

    direct_links: List[str] = []
    for a in soup.select("a[href]"):
        href = urljoin(BASE_URL, a.get("href", "").strip())
        txt = clean_text(a.get_text(" ", strip=True))
        if not href:
            continue
        domain = get_domain(href)
        if any(x in domain for x in ["google.com", "bing.com", "duckduckgo.com"]):
            continue
        if txt and any(k in txt for k in ["申請文件", "主辦單位", "官方", "計畫網站", "公告", "簡章", "附件"]):
            direct_links.append(href)

    organizer_site_url_raw = find_google_search_link(soup)
    if not organizer_site_url_raw and direct_links:
        organizer_site_url_raw = direct_links[0]

    item = DetailItem(
        title=title,
        detail_url=detail_url,
        plan_source=plan_source,
        eligible_targets=eligible_targets,
        applicable_region=applicable_region,
        grant_amount=grant_amount,
        organizer_site_url_raw=organizer_site_url_raw,
        deadline_date=deadline_date,
        deadline_text=deadline_text,
        topic_1=topics[0] if len(topics) > 0 else "",
        topic_2=topics[1] if len(topics) > 1 else "",
        topic_3=topics[2] if len(topics) > 2 else "",
        topic_4=topics[3] if len(topics) > 3 else "",
        topic_5=topics[4] if len(topics) > 4 else "",
        plan_background=plan_background,
        key_point_1=points[0] if len(points) > 0 else "",
        key_point_2=points[1] if len(points) > 1 else "",
        key_point_3=points[2] if len(points) > 2 else "",
        key_point_4=points[3] if len(points) > 3 else "",
        key_point_5=points[4] if len(points) > 4 else "",
        application_tips=tips,
        raw_text="\n".join(core_lines),
    )

    if direct_links:
        item.organizer_site_url = direct_links[0]
        item.organizer_site_domain = get_domain(direct_links[0])
        if is_probably_government_domain(item.organizer_site_domain):
            item.official_organizer_site_url = direct_links[0]
            item.official_organizer_domain = item.organizer_site_domain
            item.official_url_status = "direct_official"
            item.official_url_confidence = "high"
        else:
            item.official_url_status = "direct_non_google"
            item.official_url_confidence = "medium"

    return item

def resolve_by_title(item: DetailItem, cache: Dict[str, Dict[str, Any]]) -> DetailItem:
    if item.official_url_status in {"direct_official", "direct_non_google"}:
        return item

    key = title_key(item.title, item.plan_source, item.applicable_region)
    cached = cache.get(key)
    if cached:
        item.organizer_search_query = cached.get("query", "")
        item.organizer_search_scope = cached.get("scope", "")
        item.organizer_site_url = cached.get("organizer_site_url", item.organizer_site_url)
        item.organizer_site_domain = cached.get("organizer_site_domain", item.organizer_site_domain)
        item.official_organizer_site_url = cached.get("official_organizer_site_url", item.official_organizer_site_url)
        item.official_organizer_domain = cached.get("official_organizer_domain", item.official_organizer_domain)
        item.official_url_status = "cache_reused_verified"
        item.official_url_confidence = cached.get("confidence", "high")
        return item

    norm = normalize_title(item.title)
    hints = build_domain_hints(item.title, item.plan_source, item.applicable_region)
    raw_query = extract_google_query(item.organizer_site_url_raw) if is_google_search_url(item.organizer_site_url_raw) else ""

    queries: List[Tuple[str, str]] = []
    base_terms = [x for x in [raw_query, norm["full"], norm["core"]] if x]
    base_terms = list(dict.fromkeys(base_terms))

    if hints:
        for d in hints[:5]:
            for term in base_terms[:3]:
                queries.append((f'"{term}" site:{d}', d))
            queries.append((f'"{norm["core"]}" 補助 site:{d}', d))
            queries.append((f'"{norm["core"]}" 公告 site:{d}', d))
            queries.append((f'"{norm["core"]}" 申請 site:{d}', d))
    else:
        for term in base_terms[:3]:
            queries.append((term, ""))

    candidates: List[CandidateURL] = []
    seen_urls = set()
    for query, scope in queries[:14]:
        item.organizer_search_query = query
        item.organizer_search_scope = scope
        urls = search_bing_html(query)
        if not urls:
            urls = search_ddg_html(query)
        for u in urls:
            if u in seen_urls:
                continue
            seen_urls.add(u)
            candidates.append(CandidateURL(url=u, source="search", scope=scope, query=query))
        if len(candidates) >= 16:
            break
        time.sleep(0.2)

    best_verified: Optional[Tuple[str, str, float, str, str]] = None
    for cand in candidates:
        ok, domain, score = verify_candidate(cand.url, item.title, hints)
        if ok:
            if best_verified is None or score > best_verified[2]:
                best_verified = (cand.url, domain, score, cand.query, cand.scope)

    if best_verified:
        url, domain, score, query, scope = best_verified
        item.organizer_search_query = query
        item.organizer_search_scope = scope
        item.organizer_site_url = url
        item.organizer_site_domain = domain
        if is_probably_government_domain(domain):
            item.official_organizer_site_url = url
            item.official_organizer_domain = domain
            item.official_url_status = "search_verified_exact"
            item.official_url_confidence = "high" if score >= 0.9 else "medium"
        else:
            item.official_url_status = "search_verified_non_gov"
            item.official_url_confidence = "medium"

        cache[key] = {
            "query": query,
            "scope": scope,
            "organizer_site_url": item.organizer_site_url,
            "organizer_site_domain": item.organizer_site_domain,
            "official_organizer_site_url": item.official_organizer_site_url,
            "official_organizer_domain": item.official_organizer_domain,
            "confidence": item.official_url_confidence,
        }
        return item

    if item.organizer_site_url_raw:
        item.organizer_site_url = item.organizer_site_url_raw
        item.organizer_site_domain = get_domain(item.organizer_site_url_raw)
    item.official_url_status = "search_no_match"
    item.official_url_confidence = "low"
    return item

