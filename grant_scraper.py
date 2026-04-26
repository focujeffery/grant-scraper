#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, quote_plus, unquote_plus, urljoin, urlparse
from urllib.request import Request, urlopen

import pandas as pd
from bs4 import BeautifulSoup
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

BASE_URL = "https://dayseechat.com"
LISTING_URL = f"{BASE_URL}/explore-grant/"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
OUTPUT_XLSX = os.environ.get("OUTPUT_XLSX", "outputs/daysee_grants.xlsx")
DELTA_XLSX = os.environ.get("DELTA_XLSX", "outputs/daysee_grants_delta_only.xlsx")
PREVIOUS_XLSX = os.environ.get("PREVIOUS_XLSX", "state/daysee_grants_latest.xlsx")
CACHE_JSON = os.environ.get("CACHE_JSON", "state/title_url_cache.json")
MAX_PAGES = int(os.environ.get("MAX_PAGES", "20"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class ListingItem:
    title: str
    detail_url: str
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


@dataclass
class DetailItem:
    title: str
    detail_url: str
    plan_source: str = ""
    eligible_targets: str = ""
    applicable_region: str = ""
    grant_amount: str = ""
    organizer_site_url_raw: str = ""
    organizer_search_query: str = ""
    organizer_search_scope: str = ""
    organizer_site_url: str = ""
    organizer_site_domain: str = ""
    official_organizer_site_url: str = ""
    official_organizer_domain: str = ""
    official_url_status: str = ""
    official_url_confidence: str = ""
    deadline_date: str = ""
    deadline_text: str = ""
    topic_1: str = ""
    topic_2: str = ""
    topic_3: str = ""
    topic_4: str = ""
    topic_5: str = ""
    plan_background: str = ""
    key_point_1: str = ""
    key_point_2: str = ""
    key_point_3: str = ""
    key_point_4: str = ""
    key_point_5: str = ""
    application_tips: str = ""
    raw_text: str = ""


def ensure_parent(path_str: str) -> None:
    Path(path_str).parent.mkdir(parents=True, exist_ok=True)


def clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def split_lines(text: str) -> List[str]:
    out = []
    for raw in text.splitlines():
        t = clean_text(raw)
        if t:
            out.append(t)
    return out


def normalize_label_line(line: str) -> str:
    line = clean_text(line)
    line = line.lstrip("*•·- ").strip()
    line = re.sub(r"\s*[:：]\s*", "：", line)
    return line


def get_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def is_google_search_url(url: str) -> bool:
    d = get_domain(url)
    return "google." in d and "/search" in url


def extract_google_query(url: str) -> str:
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        return clean_text(unquote_plus(qs.get("q", [""])[0]))
    except Exception:
        return ""


def fetch_url(url: str, timeout: int = 25) -> Tuple[int, str, str]:
    try:
        req = Request(url, headers={"User-Agent": UA, "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8"})
        with urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 200)
            final_url = getattr(resp, "url", url)
            body = resp.read().decode("utf-8", errors="ignore")
            return status, final_url, body
    except Exception:
        return 0, url, ""


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
            if "你可能也會喜歡" in line:
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

    title_links: List[Tuple[str, str]] = []
    for a in soup.select('a[href*="/subsidy/grant-"]'):
        href = urljoin(BASE_URL, a.get("href", "").strip())
        title = clean_text(a.get_text(" ", strip=True))
        if href and title and href not in seen_urls:
            title_links.append((title, href))

    results: List[ListingItem] = []
    cursor = 0
    for idx, (title, href) in enumerate(title_links):
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
        prev_lines = lines[max(0, title_idx - 4):title_idx]

        grant_amount = ""
        for prev in reversed(prev_lines):
            prev_n = normalize_label_line(prev)
            if prev_n.startswith("補助金額："):
                grant_amount = clean_text(prev_n.replace("補助金額：", "", 1))
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
            grant_amount=grant_amount,
            deadline_date=deadline_date,
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


async def extract_all_listings(page) -> List[ListingItem]:
    await page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(2500)
    seen_urls: set[str] = set()
    all_items: List[ListingItem] = []
    target_total = 0
    no_growth = 0

    for page_no in range(1, MAX_PAGES + 1):
        logger.info("Scanning listing page %d", page_no)
        html = await page.content()
        if not target_total:
            target_total = parse_total_results(html)
            if target_total:
                logger.info("Target total grants detected: %d", target_total)

        before = len(seen_urls)
        items = parse_listing_page(html, seen_urls)
        if items:
            all_items.extend(items)
        current = len(seen_urls)
        logger.info("Collected so far: %d", current)

        if target_total and current >= target_total:
            logger.info("Reached target total %d. Stop pagination.", target_total)
            break

        if current == before:
            no_growth += 1
        else:
            no_growth = 0

        # click next and wait until count grows
        clicked = False
        btn = page.locator('a.ts-load-next:not(.ts-btn-disabled)')
        try:
            if await btn.count() > 0:
                await btn.last.scroll_into_view_if_needed(timeout=2000)
                try:
                    await btn.last.click(force=True, timeout=4000)
                except Exception:
                    handle = await btn.last.element_handle()
                    if handle:
                        await page.evaluate("(el) => el.click()", handle)
                clicked = True
        except Exception:
            clicked = False

        if not clicked:
            logger.info("No usable next button found; stop pagination.")
            break

        progressed = False
        for _ in range(10):
            await page.wait_for_timeout(700)
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=2000)
            except Exception:
                pass
            probe_html = await page.content()
            probe_seen = set(seen_urls)
            _ = parse_listing_page(probe_html, probe_seen)
            if len(probe_seen) > current:
                progressed = True
                break

        if not progressed:
            no_growth += 1
            logger.info("Pagination did not advance after click (%d/3)", no_growth)
            if no_growth >= 3:
                logger.info("Stop pagination because there is no further progress.")
                break

    return all_items


def find_google_search_link(soup: BeautifulSoup) -> str:
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        full = urljoin(BASE_URL, href)
        if is_google_search_url(full):
            return full
    return ""


def is_probably_government_domain(domain: str) -> bool:
    d = domain.lower()
    gov_markers = [
        ".gov.tw", ".gov", "gov.taipei", "ntpc.gov.tw", "kcg.gov.tw", "taichung.gov.tw",
        "tainan.gov.tw", "klcg.gov.tw", "hccg.gov.tw", "cyhg.gov.tw", "nantou.gov.tw",
        "miaoli.gov.tw", "hsinchu.gov.tw", "pif.gov.tw", "penghu.gov.tw", "moe.gov.tw",
        "mol.gov.tw", "wda.gov.tw", "moc.gov.tw", "moea.gov.tw", "moda.gov.tw", "oac.gov.tw",
        "ndc.gov.tw", "coa.gov.tw", "abo.gov.tw", "hakka.gov.tw"
    ]
    return any(m in d for m in gov_markers)


def parse_detail_page(html: str, detail_url: str) -> DetailItem:
    soup = BeautifulSoup(html, "html.parser")
    lines = split_lines(soup.get_text("\n", strip=True))

    # cut off recommendation/footer area
    cut_idx = len(lines)
    for i, line in enumerate(lines):
        if "你可能也會喜歡" in line or line.startswith("返回主頁"):
            cut_idx = i
            break
    lines = lines[:cut_idx]

    title = ""
    for sel in ["h1", "h2", "title"]:
        node = soup.select_one(sel)
        if node and clean_text(node.get_text(" ", strip=True)):
            title = clean_text(node.get_text(" ", strip=True))
            break
    if title.endswith("｜小社區大事件"):
        title = clean_text(title.replace("｜小社區大事件", ""))

    def after(label: str, stops: Sequence[str]) -> List[str]:
        return extract_after_label(lines, label, stops)

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
    for line in lines:
        line_n = normalize_label_line(line)
        if line_n.startswith("截止日期："):
            payload = clean_text(line_n.replace("截止日期：", "", 1))
            m = re.search(r"(\d{4}-\d{2}-\d{2})", payload)
            if m:
                deadline_date = m.group(1)
                deadline_text = payload.replace(deadline_date, "").strip(" ｜|")
            else:
                deadline_text = payload
            break

    topics = after("關注議題：", ["申請文件：", "計畫背景：", "計畫重點：", "撰寫技巧："])
    plan_background = "\n".join(after("計畫背景：", ["計畫重點：", "撰寫技巧：", "申請文件："]))
    key_points = after("計畫重點：", ["撰寫技巧：", "申請文件："])
    tips = "\n".join(after("撰寫技巧：", ["申請文件："]))

    direct_links = []
    for a in soup.select("a[href]"):
        href = urljoin(BASE_URL, a.get("href", "").strip())
        txt = clean_text(a.get_text(" ", strip=True))
        if not href:
            continue
        domain = get_domain(href)
        if any(x in domain for x in ["google.com", "bing.com", "duckduckgo.com", "104.com.tw"]):
            continue
        if txt and any(k in txt for k in ["申請文件", "主辦單位", "官方", "計畫網站", "公告", "簡章", "附件"]):
            direct_links.append(href)

    raw_org = find_google_search_link(soup)
    if not raw_org and direct_links:
        raw_org = direct_links[0]

    item = DetailItem(
        title=title,
        detail_url=detail_url,
        plan_source=plan_source,
        eligible_targets=eligible_targets,
        applicable_region=applicable_region,
        grant_amount=grant_amount,
        organizer_site_url_raw=raw_org,
        deadline_date=deadline_date,
        deadline_text=deadline_text,
        topic_1=topics[0] if len(topics) > 0 else "",
        topic_2=topics[1] if len(topics) > 1 else "",
        topic_3=topics[2] if len(topics) > 2 else "",
        topic_4=topics[3] if len(topics) > 3 else "",
        topic_5=topics[4] if len(topics) > 4 else "",
        plan_background=plan_background,
        key_point_1=key_points[0] if len(key_points) > 0 else "",
        key_point_2=key_points[1] if len(key_points) > 1 else "",
        key_point_3=key_points[2] if len(key_points) > 2 else "",
        key_point_4=key_points[3] if len(key_points) > 3 else "",
        key_point_5=key_points[4] if len(key_points) > 4 else "",
        application_tips=tips,
        raw_text="\n".join(lines),
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


def normalize_title(title: str) -> Dict[str, str]:
    full_title = clean_text(title)
    core = full_title
    core = re.sub(r"^\d{3,4}(年度|年)", "", core)
    core = re.sub(r"^20\d{2}", "", core)
    core = re.sub(r"[（(][^)）]*(公告|徵件|修正|補助要點|申請須知)[^)）]*[）)]", "", core)
    core = re.sub(r"\s+", " ", core).strip(" ｜|-_")
    return {"full": full_title, "core": core or full_title}


def build_domain_hints(title: str, plan_source: str, applicable_region: str) -> List[str]:
    text = f"{title} {plan_source} {applicable_region}"
    hints: List[str] = []
    exact = {
        "勞動部": ["www.mol.gov.tw", "wlb.mol.gov.tw", "ws.wda.gov.tw"],
        "教育部": ["www.moe.gov.tw"],
        "文化部": ["www.moc.gov.tw"],
        "經濟部": ["www.moea.gov.tw"],
        "數位發展部": ["www.moda.gov.tw", "digiplus.adi.gov.tw"],
        "原住民族委員會": ["www.cip.gov.tw"],
        "客家委員會": ["www.hakka.gov.tw"],
        "海洋委員會": ["www.oac.gov.tw"],
        "國家發展委員會": ["www.ndc.gov.tw"],
    }
    local = {
        "台北": ["www.gov.taipei"], "臺北": ["www.gov.taipei"],
        "新北": ["www.ntpc.gov.tw", "www.economic.ntpc.gov.tw", "www.culture.ntpc.gov.tw", "www.sw.ntpc.gov.tw"],
        "台中": ["www.taichung.gov.tw"], "臺中": ["www.taichung.gov.tw"],
        "台南": ["www.tainan.gov.tw"], "臺南": ["www.tainan.gov.tw"],
        "高雄": ["www.kcg.gov.tw"],
        "基隆": ["www.klcg.gov.tw"],
        "新竹市": ["youthhsinchu.hccg.gov.tw", "www.hccg.gov.tw"],
        "新竹縣": ["www.hsinchu.gov.tw"],
        "彰化": ["www.chcg.gov.tw"],
    }
    for k, vals in exact.items():
        if k in text:
            hints.extend(vals)
    for k, vals in local.items():
        if k in text:
            hints.extend(vals)
    # generic plan_source fallback
    if "縣市政府" in plan_source and not hints:
        for k, vals in local.items():
            if k in title or k in applicable_region:
                hints.extend(vals)
    # de-dup
    out = []
    seen = set()
    for h in hints:
        if h not in seen:
            out.append(h)
            seen.add(h)
    return out


def title_key(title: str, plan_source: str, applicable_region: str) -> str:
    norm = normalize_title(title)
    return f"{norm['core']}|{clean_text(plan_source)}|{clean_text(applicable_region)}".lower()


def search_html(url: str) -> List[str]:
    status, final_url, body = fetch_url(url, timeout=25)
    if status < 200 or status >= 400 or not body:
        return []
    soup = BeautifulSoup(body, "html.parser")
    urls = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if href.startswith("http") and not any(x in href for x in ["google.com", "bing.com", "duckduckgo.com", "facebook.com", "104.com.tw"]):
            urls.append(href)
    # dedupe preserve order
    out = []
    seen = set()
    for u in urls:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out[:10]


def search_bing_html(query: str) -> List[str]:
    return search_html(f"https://www.bing.com/search?q={quote_plus(query)}")


def search_ddg_html(query: str) -> List[str]:
    return search_html(f"https://html.duckduckgo.com/html/?q={quote_plus(query)}")


def verify_candidate(url: str, title: str, hints: List[str]) -> Tuple[bool, str, float]:
    status, final_url, body = fetch_url(url, timeout=20)
    if status < 200 or status >= 400 or not body:
        return False, get_domain(final_url), 0.0
    domain = get_domain(final_url)
    if any(x in domain for x in ["google.com", "bing.com", "duckduckgo.com", "104.com.tw"]):
        return False, domain, 0.0

    page = BeautifulSoup(body, "html.parser")
    title_text = clean_text(page.title.get_text(" ", strip=True) if page.title else "")
    body_text = clean_text(page.get_text(" ", strip=True))[:5000]

    norm = normalize_title(title)
    score = 0.0
    if norm["full"] and norm["full"] in body_text:
        score += 1.0
    if norm["core"] and norm["core"] in body_text:
        score += 0.8
    core_tokens = [x for x in re.split(r"[\s｜|、,，()（）]+", norm["core"]) if len(x) >= 2][:5]
    token_hits = sum(1 for t in core_tokens if t in body_text or t in title_text)
    score += min(0.6, token_hits * 0.15)
    if is_probably_government_domain(domain):
        score += 0.5
    if hints and any(h.replace("www.", "") in domain for h in hints):
        score += 0.5
    return score >= 0.9, domain, score


def load_cache() -> Dict[str, Dict[str, Any]]:
    path = Path(CACHE_JSON)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    ensure_parent(CACHE_JSON)
    Path(CACHE_JSON).write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def resolve_by_title(item: DetailItem, cache: Dict[str, Dict[str, Any]]) -> DetailItem:
    if item.official_url_status in {"direct_official", "direct_non_google"}:
        return item

    key = title_key(item.title, item.plan_source, item.applicable_region)
    if key in cache:
        c = cache[key]
        item.organizer_search_query = c.get("query", "")
        item.organizer_search_scope = c.get("scope", "")
        item.organizer_site_url = c.get("organizer_site_url", "")
        item.organizer_site_domain = c.get("organizer_site_domain", "")
        item.official_organizer_site_url = c.get("official_organizer_site_url", "")
        item.official_organizer_domain = c.get("official_organizer_domain", "")
        item.official_url_status = "cache_reused_verified"
        item.official_url_confidence = c.get("confidence", "high")
        return item

    norm = normalize_title(item.title)
    raw_query = extract_google_query(item.organizer_site_url_raw) if is_google_search_url(item.organizer_site_url_raw) else ""
    hints = build_domain_hints(item.title, item.plan_source, item.applicable_region)
    base_terms = [x for x in [raw_query, norm["full"], norm["core"]] if x]
    base_terms = list(dict.fromkeys(base_terms))

    queries: List[Tuple[str, str]] = []
    for d in hints[:5]:
        for term in base_terms[:3]:
            queries.append((f'"{term}" site:{d}', d))
        queries.append((f'"{norm["core"]}" 補助 site:{d}', d))
        queries.append((f'"{norm["core"]}" 公告 site:{d}', d))
    if not queries:
        for term in base_terms[:3]:
            queries.append((term, ""))

    candidates: List[Tuple[str, str, str]] = []
    seen = set()
    for query, scope in queries[:12]:
        item.organizer_search_query = query
        item.organizer_search_scope = scope
        urls = search_bing_html(query) or search_ddg_html(query)
        for u in urls:
            if u not in seen:
                candidates.append((u, query, scope))
                seen.add(u)
        if len(candidates) >= 16:
            break
        time.sleep(0.2)

    best = None
    for u, q, s in candidates:
        ok, domain, score = verify_candidate(u, item.title, hints)
        if ok and (best is None or score > best[2]):
            best = (u, domain, score, q, s)

    if best:
        url, domain, score, query, scope = best
        item.organizer_search_query = query
        item.organizer_search_scope = scope
        item.organizer_site_url = url
        item.organizer_site_domain = domain
        if is_probably_government_domain(domain):
            item.official_organizer_site_url = url
            item.official_organizer_domain = domain
            item.official_url_status = "search_verified_exact"
            item.official_url_confidence = "high" if score >= 1.2 else "medium"
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


def detail_from_listing(item: ListingItem, status: str) -> DetailItem:
    return DetailItem(
        title=item.title,
        detail_url=item.detail_url,
        plan_source=item.plan_source,
        eligible_targets=item.eligible_targets,
        grant_amount=item.grant_amount,
        deadline_date=item.deadline_date,
        deadline_text=item.deadline_text,
        topic_1=item.topic_1,
        topic_2=item.topic_2,
        topic_3=item.topic_3,
        topic_4=item.topic_4,
        topic_5=item.topic_5,
        organizer_site_url=item.detail_url,
        organizer_site_domain=get_domain(item.detail_url),
        official_url_status=status,
    )


async def extract_detail(page, item: ListingItem) -> DetailItem:
    logger.info("Fetching detail: %s", item.detail_url)
    last_exc = None
    for attempt in range(1, 4):
        try:
            await page.goto(item.detail_url, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1500)
            html = await page.content()
            return parse_detail_page(html, item.detail_url)
        except PlaywrightTimeoutError as exc:
            last_exc = exc
            logger.warning("Detail timeout for %s (attempt %d/3)", item.detail_url, attempt)
            try:
                await page.goto("about:blank", wait_until="load", timeout=10000)
            except Exception:
                pass
            await page.wait_for_timeout(1200 * attempt)
        except Exception as exc:
            last_exc = exc
            logger.warning("Detail fetch failed for %s (attempt %d/3): %s", item.detail_url, attempt, exc)
            try:
                await page.goto("about:blank", wait_until="load", timeout=10000)
            except Exception:
                pass
            await page.wait_for_timeout(1200 * attempt)
    logger.error("Fallback to listing-only detail for %s because detail page could not be read: %s", item.detail_url, last_exc)
    return detail_from_listing(item, "detail_timeout_fallback")


def load_previous_summary() -> Optional[pd.DataFrame]:
    if not PREVIOUS_XLSX or not Path(PREVIOUS_XLSX).exists():
        return None
    try:
        return pd.read_excel(PREVIOUS_XLSX, sheet_name="grants_summary")
    except Exception:
        return None


def build_delta(current_df: pd.DataFrame, previous_df: Optional[pd.DataFrame]):
    key_col = "detail_url"
    if previous_df is None or previous_df.empty:
        stats = {
            "current_count": int(len(current_df)),
            "previous_count": 0,
            "new_count": int(len(current_df)),
            "updated_count": 0,
            "removed_count": 0,
        }
        return current_df.copy(), current_df.iloc[0:0].copy(), current_df.iloc[0:0].copy(), stats

    cur = current_df.fillna("").copy()
    prev = previous_df.fillna("").copy()
    cur_idx = cur.set_index(key_col, drop=False)
    prev_idx = prev.set_index(key_col, drop=False)

    new_keys = [k for k in cur_idx.index if k not in prev_idx.index]
    removed_keys = [k for k in prev_idx.index if k not in cur_idx.index]

    compare_cols = [c for c in cur.columns if c in prev.columns and c != key_col]
    updated_keys = []
    for k in cur_idx.index.intersection(prev_idx.index):
        if any(str(cur_idx.at[k, c]) != str(prev_idx.at[k, c]) for c in compare_cols):
            updated_keys.append(k)

    new_df = cur_idx.loc[new_keys].reset_index(drop=True) if new_keys else cur.iloc[0:0].copy()
    updated_df = cur_idx.loc[updated_keys].reset_index(drop=True) if updated_keys else cur.iloc[0:0].copy()
    removed_df = prev_idx.loc[removed_keys].reset_index(drop=True) if removed_keys else prev.iloc[0:0].copy()
    stats = {
        "current_count": int(len(cur)),
        "previous_count": int(len(prev)),
        "new_count": int(len(new_df)),
        "updated_count": int(len(updated_df)),
        "removed_count": int(len(removed_df)),
    }
    return new_df, updated_df, removed_df, stats


def style_sheet(ws, wrap_cols: Optional[Sequence[int]] = None):
    wrap_cols = set(wrap_cols or [])
    ws.freeze_panes = "A2"
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(vertical="top")
    widths = {}
    for row in ws.iter_rows():
        for cell in row:
            if cell.value is None:
                continue
            val = str(cell.value)
            widths[cell.column] = min(48, max(widths.get(cell.column, 10), len(val[:50]) + 2))
    for col_idx, width in widths.items():
        ws.column_dimensions[get_column_letter(col_idx)].width = width
    for row in ws.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(wrap_text=(cell.column in wrap_cols), vertical="top")


def write_workbooks(detail_items: List[DetailItem], previous_df: Optional[pd.DataFrame]) -> Dict[str, int]:
    rows = [asdict(x) for x in detail_items]
    detail_df = pd.DataFrame(rows)

    summary_cols = [
        "title", "detail_url", "plan_source", "eligible_targets", "applicable_region",
        "grant_amount", "deadline_date", "deadline_text",
        "topic_1", "topic_2", "topic_3", "topic_4", "topic_5",
        "organizer_site_url", "organizer_site_domain",
        "official_organizer_site_url", "official_organizer_domain",
        "official_url_status", "official_url_confidence",
    ]
    for c in summary_cols:
        if c not in detail_df.columns:
            detail_df[c] = ""
    summary_df = detail_df[summary_cols].copy()

    new_df, updated_df, removed_df, stats = build_delta(summary_df, previous_df)

    ensure_parent(OUTPUT_XLSX)
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="grants_summary", index=False)
        detail_df.to_excel(writer, sheet_name="grants_detail", index=False)
        pd.DataFrame([stats]).to_excel(writer, sheet_name="weekly_changes_summary", index=False)
        new_df.to_excel(writer, sheet_name="weekly_new_plans", index=False)
        updated_df.to_excel(writer, sheet_name="weekly_updated_plans", index=False)
        removed_df.to_excel(writer, sheet_name="weekly_removed_plans", index=False)

        wb = writer.book
        style_sheet(wb["grants_summary"], wrap_cols=[])
        style_sheet(wb["grants_detail"], wrap_cols=[detail_df.columns.get_loc(c)+1 for c in ["plan_background", "application_tips", "raw_text"] if c in detail_df.columns])
        style_sheet(wb["weekly_changes_summary"], wrap_cols=[])
        style_sheet(wb["weekly_new_plans"], wrap_cols=[])
        style_sheet(wb["weekly_updated_plans"], wrap_cols=[])
        style_sheet(wb["weekly_removed_plans"], wrap_cols=[])

    ensure_parent(DELTA_XLSX)
    with pd.ExcelWriter(DELTA_XLSX, engine="openpyxl") as writer:
        pd.DataFrame([stats]).to_excel(writer, sheet_name="changes_summary", index=False)
        new_df.to_excel(writer, sheet_name="new_plans", index=False)
        updated_df.to_excel(writer, sheet_name="updated_plans", index=False)
        removed_df.to_excel(writer, sheet_name="removed_plans", index=False)
        wb = writer.book
        style_sheet(wb["changes_summary"], wrap_cols=[])
        style_sheet(wb["new_plans"], wrap_cols=[])
        style_sheet(wb["updated_plans"], wrap_cols=[])
        style_sheet(wb["removed_plans"], wrap_cols=[])

    return stats


async def async_main() -> None:
    cache = load_cache()
    previous_df = load_previous_summary()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=UA, locale="zh-TW")
        list_page = await context.new_page()
        listing_items = await extract_all_listings(list_page)
        await list_page.close()
        logger.info("Found %d grants", len(listing_items))

        detail_page = await context.new_page()
        detail_items: List[DetailItem] = []
        for item in listing_items:
            detail = await extract_detail(detail_page, item)
            detail = resolve_by_title(detail, cache)
            if not detail.plan_source:
                detail.plan_source = item.plan_source
            if not detail.eligible_targets:
                detail.eligible_targets = item.eligible_targets
            if not detail.grant_amount:
                detail.grant_amount = item.grant_amount
            if not detail.deadline_date:
                detail.deadline_date = item.deadline_date
            if not detail.topic_1:
                detail.topic_1 = item.topic_1
                detail.topic_2 = item.topic_2
                detail.topic_3 = item.topic_3
                detail.topic_4 = item.topic_4
                detail.topic_5 = item.topic_5
            detail_items.append(detail)
            await asyncio.sleep(0.2)
        await detail_page.close()
        await context.close()
        await browser.close()

    stats = write_workbooks(detail_items, previous_df)
    save_cache(cache)
    logger.info("Exported %d records to %s", len(detail_items), OUTPUT_XLSX)
    print(json.dumps(stats, ensure_ascii=False))


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
