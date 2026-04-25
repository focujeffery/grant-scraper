import asyncio
import json
import logging
import os
import re
import shutil
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://dayseechat.com"
LISTING_URL = f"{BASE_URL}/explore-grant/"
OUTPUT_XLSX = os.environ.get("OUTPUT_XLSX", "outputs/daysee_grants.xlsx")
DELTA_XLSX = os.environ.get("DELTA_XLSX", "outputs/daysee_grants_delta_only.xlsx")
STATE_DIR = Path(os.environ.get("STATE_DIR", "state"))
PREVIOUS_SNAPSHOT = Path(os.environ.get("PREVIOUS_XLSX", str(STATE_DIR / "daysee_grants_previous.xlsx")))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "25"))

OFFICIAL_PORTAL_BY_SOURCE = {
    "勞動部": "https://www.mol.gov.tw/",
    "數位發展部": "https://moda.gov.tw/",
    "客家委員會": "https://www.hakka.gov.tw/",
    "原住民族委員會": "https://www.cip.gov.tw/",
    "國家發展委員會": "https://www.ndc.gov.tw/",
    "海洋委員會": "https://www.oac.gov.tw/",
    "文化部": "https://www.moc.gov.tw/",
    "教育部": "https://www.edu.tw/",
    "經濟部": "https://www.moea.gov.tw/",
    "衛生福利部": "https://www.mohw.gov.tw/",
    "農業部": "https://www.moa.gov.tw/",
    "環境部": "https://www.moenv.gov.tw/",
    "內政部": "https://www.moi.gov.tw/",
    "交通部": "https://www.motc.gov.tw/",
    "外交部": "https://www.mofa.gov.tw/",
    "法務部": "https://www.moj.gov.tw/",
    "體育署": "https://www.sa.gov.tw/",
    "觀光署": "https://www.taiwan.net.tw/",
    "中小及新創企業署": "https://www.sme.gov.tw/",
}

OFFICIAL_PORTAL_BY_REGION = {
    "臺北市": "https://www.gov.taipei/",
    "台北市": "https://www.gov.taipei/",
    "新北市": "https://www.newtaipei.gov.tw/",
    "桃園市": "https://www.tycg.gov.tw/",
    "臺中市": "https://www.taichung.gov.tw/",
    "台中市": "https://www.taichung.gov.tw/",
    "臺南市": "https://www.tainan.gov.tw/",
    "台南市": "https://www.tainan.gov.tw/",
    "高雄市": "https://www.kcg.gov.tw/",
    "基隆市": "https://www.klcg.gov.tw/",
    "新竹市": "https://www.hccg.gov.tw/",
    "新竹縣": "https://www.hsinchu.gov.tw/",
    "苗栗縣": "https://www.miaoli.gov.tw/",
    "彰化縣": "https://www.chcg.gov.tw/",
    "南投縣": "https://www.nantou.gov.tw/",
    "雲林縣": "https://www.yunlin.gov.tw/",
    "嘉義市": "https://www.chiayi.gov.tw/",
    "嘉義縣": "https://www.cyhg.gov.tw/",
    "屏東縣": "https://www.pthg.gov.tw/",
    "宜蘭縣": "https://www.ilan.gov.tw/",
    "花蓮縣": "https://www.hl.gov.tw/",
    "臺東縣": "https://www.taitung.gov.tw/",
    "台東縣": "https://www.taitung.gov.tw/",
    "澎湖縣": "https://www.penghu.gov.tw/",
    "金門縣": "https://www.kinmen.gov.tw/",
    "連江縣": "https://www.matsu.gov.tw/",
}

MANUAL_OVERRIDES = {
    "115年促轉基金還原歷史真相旗艦計畫": "https://www.ndc.gov.tw/nc_14813_40016",
    "115年補助辦理兒童及少年未來教育與發展帳戶家戶訪視計畫": "https://www.sw.ntpc.gov.tw/",
    "115年度新北市政府鼓勵廠商國內外參展補助計畫": "https://www.economic.ntpc.gov.tw/Api/News/Page?id=7127",
    "115年度新北市社區營造一般性計畫": "https://www.culture.ntpc.gov.tw/",
    "2026新竹市政府青年AI數位工具補助": "https://youthhsinchu.hccg.gov.tw/",
    "115年親海無礙海洋創生行動方案": "https://www.oac.gov.tw/",
}

BAD_DOMAINS = {
    "www.google.com",
    "google.com",
    "support.google.com",
    "www.bing.com",
    "bing.com",
    "www.104.com.tw",
    "104.com.tw",
}


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
    organizer_site_url: str = ""
    organizer_site_domain: str = ""
    official_organizer_site_url: str = ""
    official_organizer_domain: str = ""
    official_url_status: str = ""
    application_note: str = ""
    deadline_date: str = ""
    deadline_text: str = ""
    topic_1: str = ""
    topic_2: str = ""
    topic_3: str = ""
    topic_4: str = ""
    topic_5: str = ""
    background: str = ""
    key_point_1: str = ""
    key_point_2: str = ""
    key_point_3: str = ""
    key_point_4: str = ""
    key_point_5: str = ""
    writing_tips: str = ""
    raw_text: str = ""


def clean_text(value: str) -> str:
    if value is None:
        return ""
    value = str(value).replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def split_lines(text: str) -> List[str]:
    lines = []
    for raw in text.splitlines():
        s = raw.strip().strip("•").strip("▪").strip("-").strip()
        if s:
            lines.append(s)
    return lines


def parse_deadline_and_topics(lines: List[str], start_idx: int) -> Tuple[str, List[str]]:
    date_value = ""
    topics: List[str] = []
    for idx in range(start_idx + 1, min(len(lines), start_idx + 8)):
        line = lines[idx]
        if re.match(r"^\d{4}-\d{2}-\d{2}$", line):
            date_value = line
            continue
        if line.startswith("#本資訊") or line.startswith("▎") or line.startswith("計畫簡介"):
            break
        if not any(x in line for x in ["計畫", "補助", "來源", "對象", "金額", "地區"]):
            topics.append(line)
    return date_value, topics[:5]


def extract_after_label(lines: List[str], label: str, stop_labels: List[str]) -> List[str]:
    results: List[str] = []
    active = False
    for line in lines:
        if line.startswith(label):
            active = True
            continue
        if active and any(line.startswith(stop) for stop in stop_labels):
            break
        if active:
            results.append(line)
    return results


def extract_sections_from_text(text: str) -> Dict[str, str]:
    sections: Dict[str, str] = {}
    patterns = [
        ("background", r"###\s*計畫背景\s*(.*?)\s*(?=###\s*計畫重點|###\s*撰寫技巧|$)"),
        ("key_points", r"###\s*計畫重點\s*(.*?)\s*(?=###\s*撰寫技巧|$)"),
        ("writing_tips", r"###\s*撰寫技巧\s*(.*?)\s*(?=返回主頁|##\s*你可能也會喜歡|$)"),
    ]
    for key, pat in patterns:
        m = re.search(pat, text, re.S)
        sections[key] = m.group(1).strip() if m else ""
    return sections


def parse_listing_page(html: str, seen_urls: set) -> List[ListingItem]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[ListingItem] = []
    for a in soup.select('a[href*="/subsidy/grant-"]'):
        href = urljoin(BASE_URL, a.get("href", "").strip())
        if not href or href in seen_urls:
            continue
        title = clean_text(a.get_text(" ", strip=True))
        if not title:
            continue
        block = a
        for _ in range(8):
            txt = block.get_text("\n", strip=True) if hasattr(block, "get_text") else ""
            if "截止日期" in txt and ("計畫來源" in txt or "補助對象" in txt):
                break
            if getattr(block, "parent", None) is None:
                break
            block = block.parent
        lines = split_lines(block.get_text("\n", strip=True))
        topics = extract_after_label(lines, "＃關注議題：", ["＃補助對象：", "＃計畫來源：", "＃補助金額：", "截止日期："])
        recipients = extract_after_label(lines, "＃補助對象：", ["＃計畫來源：", "＃補助金額：", "截止日期："])
        sources = extract_after_label(lines, "＃計畫來源：", ["＃補助金額：", "截止日期："])
        amounts = extract_after_label(lines, "＃補助金額：", ["截止日期："])
        deadline_date = ""
        for line in lines:
            if line.startswith("截止日期："):
                m = re.search(r"(\d{4}-\d{2}-\d{2})", line)
                if m:
                    deadline_date = m.group(1)
                break
        item = ListingItem(
            title=title,
            detail_url=href,
            plan_source="｜".join(sources),
            eligible_targets="｜".join(recipients),
            applicable_region="",
            grant_amount="｜".join([x for x in amounts if not x.isdigit()]),
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
    return results


def parse_total_results(html: str) -> int:
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    m = re.search(r"(\d+)\s*out\s*of\s*(\d+)\s*results", text, re.I)
    if m:
        return int(m.group(2))
    return 0


def get_domain(url: str) -> str:
    if not url:
        return ""
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def extract_google_query(url: str) -> str:
    if not url or "google.com/search" not in url:
        return ""
    try:
        return parse_qs(urlparse(url).query).get("q", [""])[0]
    except Exception:
        return ""


def region_hint_from_text(title: str, applicable_region: str) -> str:
    text = f"{title} {applicable_region}"
    for key in OFFICIAL_PORTAL_BY_REGION:
        if key in text:
            return key
    return ""


def choose_official_urls(title: str, plan_source: str, applicable_region: str, raw_url: str) -> Tuple[str, str, str, str, str]:
    raw_url = clean_text(raw_url)
    search_query = extract_google_query(raw_url) or title
    if title in MANUAL_OVERRIDES:
        url = MANUAL_OVERRIDES[title]
        dom = get_domain(url)
        return raw_url, search_query, url, dom, "manual_verified_override"

    if raw_url and raw_url.startswith("http") and get_domain(raw_url) not in BAD_DOMAINS:
        dom = get_domain(raw_url)
        status = "direct_official" if ".gov" in dom or dom.endswith("gov.tw") else "direct_non_google"
        return raw_url, search_query, raw_url, dom, status

    if plan_source in OFFICIAL_PORTAL_BY_SOURCE:
        url = OFFICIAL_PORTAL_BY_SOURCE[plan_source]
        return raw_url, search_query, url, get_domain(url), "verified_portal_homepage"

    region = region_hint_from_text(title, applicable_region)
    if region and region in OFFICIAL_PORTAL_BY_REGION:
        url = OFFICIAL_PORTAL_BY_REGION[region]
        return raw_url, search_query, url, get_domain(url), "verified_portal_homepage"

    return raw_url, search_query, raw_url, "", "bing_title_no_match"


def parse_detail_page(html: str, detail_url: str) -> DetailItem:
    soup = BeautifulSoup(html, "html.parser")
    title = clean_text((soup.select_one("h1") or soup.select_one("h2") or soup.select_one("title")).get_text(" ", strip=True))

    top_text = soup.get_text("\n", strip=True)
    lines = split_lines(top_text)

    source_vals = extract_after_label(lines, "計畫來源 :", ["補助對象 :", "適用地區 :", "補助金額 :", "截止日期 :"])
    target_vals = extract_after_label(lines, "補助對象 :", ["適用地區 :", "補助金額 :", "截止日期 :"])
    region_vals = extract_after_label(lines, "適用地區 :", ["補助金額 :", "截止日期 :"])
    amount_vals = extract_after_label(lines, "補助金額 :", ["截止日期 :"])

    deadline_date = ""
    topics: List[str] = []
    for idx, line in enumerate(lines):
        if line.startswith("截止日期 :") or line.startswith("截止日期："):
            deadline_date, topics = parse_deadline_and_topics(lines, idx)
            break

    app_anchor = None
    for a in soup.select("a[href]"):
        txt = clean_text(a.get_text(" ", strip=True))
        href = a.get("href", "")
        if "申請文件" in txt or "主辦單位網站" in txt:
            app_anchor = (txt, urljoin(BASE_URL, href))
            break
    application_note = app_anchor[0] if app_anchor else ""
    organizer_raw = app_anchor[1] if app_anchor else ""

    sections = extract_sections_from_text(top_text)
    key_points = split_lines(sections.get("key_points", ""))[:5]

    raw_url, search_query, final_url, final_domain, status = choose_official_urls(
        title=title,
        plan_source="｜".join(source_vals),
        applicable_region="｜".join(region_vals),
        raw_url=organizer_raw,
    )

    official_url = final_url if status != "bing_title_no_match" else ""
    official_domain = final_domain if status != "bing_title_no_match" else ""

    return DetailItem(
        title=title,
        detail_url=detail_url,
        plan_source="｜".join(source_vals),
        eligible_targets="｜".join(target_vals),
        applicable_region="｜".join(region_vals),
        grant_amount="｜".join([x for x in amount_vals if not x.startswith("申請文件") and not re.match(r"^\d+$", x)]),
        organizer_site_url_raw=raw_url,
        organizer_search_query=search_query,
        organizer_site_url=final_url or raw_url,
        organizer_site_domain=get_domain(final_url or raw_url),
        official_organizer_site_url=official_url,
        official_organizer_domain=official_domain,
        official_url_status=status,
        application_note=application_note,
        deadline_date=deadline_date,
        deadline_text="｜".join(topics),
        topic_1=topics[0] if len(topics) > 0 else "",
        topic_2=topics[1] if len(topics) > 1 else "",
        topic_3=topics[2] if len(topics) > 2 else "",
        topic_4=topics[3] if len(topics) > 3 else "",
        topic_5=topics[4] if len(topics) > 4 else "",
        background=sections.get("background", "").strip(),
        key_point_1=key_points[0] if len(key_points) > 0 else "",
        key_point_2=key_points[1] if len(key_points) > 1 else "",
        key_point_3=key_points[2] if len(key_points) > 2 else "",
        key_point_4=key_points[3] if len(key_points) > 3 else "",
        key_point_5=key_points[4] if len(key_points) > 4 else "",
        writing_tips=sections.get("writing_tips", "").strip(),
        raw_text=top_text,
    )


async def extract_all_listings(page) -> List[ListingItem]:
    await page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(2500)
    seen_urls: set = set()
    all_items: List[ListingItem] = []
    target_total = 0
    stagnant_clicks = 0

    for page_no in range(1, MAX_PAGES + 1):
        logger.info("Scanning listing page %d", page_no)
        html = await page.content()
        if not target_total:
            target_total = parse_total_results(html)
            if target_total:
                logger.info("Target total grants detected: %d", target_total)

        new_items = parse_listing_page(html, seen_urls)
        if new_items:
            all_items.extend(new_items)
        current_total = len(seen_urls)
        logger.info("Collected so far: %d", current_total)

        if target_total and current_total >= target_total:
            logger.info("Reached target total %d. Stop pagination.", target_total)
            break

        next_clicked = False
        selectors = ["a.ts-load-next:not(.ts-btn-disabled)", "text=下一頁"]
        for sel in selectors:
            try:
                locator = page.locator(sel)
                if await locator.count() == 0:
                    continue
                btn = locator.last
                await btn.scroll_into_view_if_needed(timeout=1500)
                try:
                    await btn.click(force=True, timeout=3500)
                except Exception:
                    handle = await btn.element_handle()
                    if handle:
                        await page.evaluate("(el) => el.click()", handle)
                next_clicked = True
                break
            except Exception:
                continue

        if not next_clicked:
            logger.info("No usable next button found; stop pagination.")
            break

        before_click_total = current_total
        progressed = False
        for poll_round in range(1, 9):
            await page.wait_for_timeout(1200)
            try:
                await page.wait_for_load_state("networkidle", timeout=4000)
            except Exception:
                pass
            probe_html = await page.content()
            probe_seen = set(seen_urls)
            probe_items = parse_listing_page(probe_html, probe_seen)
            if len(probe_seen) > before_click_total:
                seen_urls = probe_seen
                if probe_items:
                    all_items.extend(probe_items)
                progressed = True
                logger.info("Pagination advanced after poll %d; collected so far: %d", poll_round, len(seen_urls))
                break

        if not progressed:
            stagnant_clicks += 1
            logger.info("Pagination did not produce new grants after click (%d/2)", stagnant_clicks)
        else:
            stagnant_clicks = 0

        if stagnant_clicks >= 2:
            logger.info("Stop pagination because repeated clicks produced no further growth.")
            break

    return all_items


def detail_from_listing(item: ListingItem, status: str) -> DetailItem:
    return DetailItem(
        title=item.title,
        detail_url=item.detail_url,
        plan_source=item.plan_source,
        eligible_targets=item.eligible_targets,
        applicable_region=item.applicable_region,
        grant_amount=item.grant_amount,
        organizer_site_url=item.detail_url,
        organizer_site_domain=get_domain(item.detail_url),
        official_url_status=status,
        deadline_date=item.deadline_date,
        deadline_text=item.deadline_text,
        topic_1=item.topic_1,
        topic_2=item.topic_2,
        topic_3=item.topic_3,
        topic_4=item.topic_4,
        topic_5=item.topic_5,
    )

async def extract_detail(page, item: ListingItem) -> DetailItem:
    detail_url = item.detail_url
    logger.info("Fetching detail: %s", detail_url)
    last_exc = None
    for attempt in range(1, 4):
        try:
            wait_until = "domcontentloaded" if attempt == 1 else "commit"
            timeout_ms = 90000 if attempt == 1 else 45000
            await page.goto(detail_url, wait_until=wait_until, timeout=timeout_ms)
            await page.wait_for_timeout(1500)
            html = await page.content()
            return parse_detail_page(html, detail_url)
        except PlaywrightTimeoutError as exc:
            last_exc = exc
            logger.warning("Detail timeout for %s (attempt %d/3)", detail_url, attempt)
            try:
                await page.goto("about:blank", wait_until="load", timeout=10000)
            except Exception:
                pass
            await page.wait_for_timeout(1200 * attempt)
        except Exception as exc:
            last_exc = exc
            logger.warning("Detail fetch failed for %s (attempt %d/3): %s", detail_url, attempt, exc)
            try:
                await page.goto("about:blank", wait_until="load", timeout=10000)
            except Exception:
                pass
            await page.wait_for_timeout(1200 * attempt)

    logger.error("Fallback to listing-only detail for %s because detail page could not be read: %s", detail_url, last_exc)
    return detail_from_listing(item, "detail_timeout_fallback")


def dataframe_from_details(details: List[DetailItem]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    detail_df = pd.DataFrame([asdict(d) for d in details])
    summary_cols = [
        "title", "plan_source", "eligible_targets", "applicable_region", "grant_amount",
        "deadline_date", "deadline_text", "topic_1", "topic_2", "topic_3", "topic_4", "topic_5",
        "organizer_site_url_raw", "organizer_search_query", "organizer_site_url", "organizer_site_domain",
        "official_organizer_site_url", "official_organizer_domain", "official_url_status", "detail_url",
    ]
    detail_cols = [
        "title", "detail_url", "plan_source", "eligible_targets", "applicable_region", "grant_amount",
        "organizer_site_url_raw", "organizer_search_query", "organizer_site_url", "organizer_site_domain",
        "official_organizer_site_url", "official_organizer_domain", "official_url_status", "application_note",
        "deadline_date", "deadline_text", "topic_1", "topic_2", "topic_3", "topic_4", "topic_5",
        "background", "key_point_1", "key_point_2", "key_point_3", "key_point_4", "key_point_5",
        "writing_tips", "raw_text",
    ]
    for col in summary_cols + detail_cols:
        if col not in detail_df.columns:
            detail_df[col] = ""
    summary_df = detail_df[summary_cols].copy()
    detail_df = detail_df[detail_cols].copy()
    return summary_df, detail_df


def _row_signature(row: pd.Series) -> str:
    keep_cols = [
        "title", "plan_source", "eligible_targets", "applicable_region", "grant_amount",
        "deadline_date", "deadline_text", "organizer_site_url", "official_organizer_site_url", "official_url_status"
    ]
    return "||".join(clean_text(row.get(col, "")) for col in keep_cols)


def build_delta(summary_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if PREVIOUS_SNAPSHOT.exists():
        try:
            prev_summary = pd.read_excel(PREVIOUS_SNAPSHOT, sheet_name="grants_summary")
        except Exception:
            prev_summary = pd.DataFrame(columns=summary_df.columns)
    else:
        prev_summary = pd.DataFrame(columns=summary_df.columns)

    current = summary_df.copy()
    prev = prev_summary.copy()
    for df in (current, prev):
        if "detail_url" not in df.columns:
            df["detail_url"] = ""
        df["detail_url"] = df["detail_url"].astype(str)
        df["_sig"] = df.apply(_row_signature, axis=1) if len(df) else []

    prev_map = {r["detail_url"]: r["_sig"] for _, r in prev.iterrows() if r.get("detail_url")}
    curr_map = {r["detail_url"]: r["_sig"] for _, r in current.iterrows() if r.get("detail_url")}

    new_urls = sorted(set(curr_map) - set(prev_map))
    removed_urls = sorted(set(prev_map) - set(curr_map))
    updated_urls = sorted(url for url in set(curr_map) & set(prev_map) if curr_map[url] != prev_map[url])

    new_df = current[current["detail_url"].isin(new_urls)].drop(columns=["_sig"], errors="ignore")
    updated_df = current[current["detail_url"].isin(updated_urls)].drop(columns=["_sig"], errors="ignore")
    removed_df = prev[prev["detail_url"].isin(removed_urls)].drop(columns=["_sig"], errors="ignore")

    summary = pd.DataFrame([
        {"metric": "current_count", "value": len(current)},
        {"metric": "previous_count", "value": len(prev)},
        {"metric": "new_count", "value": len(new_df)},
        {"metric": "updated_count", "value": len(updated_df)},
        {"metric": "removed_count", "value": len(removed_df)},
        {"metric": "generated_at", "value": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")},
    ])
    return summary, new_df, updated_df, removed_df


def ensure_dirs():
    Path(OUTPUT_XLSX).parent.mkdir(parents=True, exist_ok=True)
    Path(DELTA_XLSX).parent.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    PREVIOUS_SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)


def style_workbook(path: Path, summary_sheet: str, detail_sheet: str):
    wb = load_workbook(path)
    header_fill = PatternFill("solid", fgColor="1F4E78")
    header_font = Font(color="FFFFFF", bold=True)
    wrap_cols = {"background", "writing_tips", "raw_text", "application_note", "key_point_1", "key_point_2", "key_point_3", "key_point_4", "key_point_5"}
    for ws in wb.worksheets:
        ws.freeze_panes = "A2"
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")
        for col_cells in ws.columns:
            header = col_cells[0].value or ""
            max_len = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells[:150])
            width = min(max(max_len + 2, 12), 45)
            if header in wrap_cols:
                width = 42
            ws.column_dimensions[col_cells[0].column_letter].width = width
            for cell in col_cells[1:]:
                if header in wrap_cols:
                    cell.alignment = Alignment(wrap_text=True, vertical="top")
                else:
                    cell.alignment = Alignment(vertical="top")
        if ws.title == detail_sheet:
            for row in range(2, ws.max_row + 1):
                ws.row_dimensions[row].height = 36
    wb.save(path)


async def main():
    ensure_dirs()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(locale="zh-TW")
        page = await context.new_page()
        listings = await extract_all_listings(page)
        logger.info("Found %d grants", len(listings))
        # de-duplicate and keep stable order
        unique_listing_map: Dict[str, ListingItem] = {}
        for item in listings:
            unique_listing_map[item.detail_url] = item
        listings = list(unique_listing_map.values())
        listings.sort(key=lambda x: x.detail_url)

        details: List[DetailItem] = []
        detail_page = await context.new_page()
        for item in listings:
            detail = await extract_detail(detail_page, item)
            # fallback to listing fields when detail top fields are blank
            if not detail.plan_source:
                detail.plan_source = item.plan_source
            if not detail.eligible_targets:
                detail.eligible_targets = item.eligible_targets
            if not detail.grant_amount:
                detail.grant_amount = item.grant_amount
            if not detail.deadline_date:
                detail.deadline_date = item.deadline_date
            if not detail.deadline_text:
                detail.deadline_text = item.deadline_text
            for i in range(1, 6):
                if not getattr(detail, f"topic_{i}"):
                    setattr(detail, f"topic_{i}", getattr(item, f"topic_{i}"))
            details.append(detail)
        await context.close()
        await browser.close()

    summary_df, detail_df = dataframe_from_details(details)
    summary_df = summary_df.drop_duplicates(subset=["detail_url"]).sort_values(by=["deadline_date", "title"], na_position="last")
    detail_df = detail_df.drop_duplicates(subset=["detail_url"]).sort_values(by=["deadline_date", "title"], na_position="last")

    changes_summary, new_df, updated_df, removed_df = build_delta(summary_df)

    output_path = Path(OUTPUT_XLSX)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="grants_summary", index=False)
        detail_df.to_excel(writer, sheet_name="grants_detail", index=False)
        changes_summary.to_excel(writer, sheet_name="weekly_changes_summary", index=False)
        new_df.to_excel(writer, sheet_name="weekly_new_plans", index=False)
        updated_df.to_excel(writer, sheet_name="weekly_updated_plans", index=False)
        removed_df.to_excel(writer, sheet_name="weekly_removed_plans", index=False)
    style_workbook(output_path, "grants_summary", "grants_detail")

    delta_path = Path(DELTA_XLSX)
    with pd.ExcelWriter(delta_path, engine="openpyxl") as writer:
        changes_summary.to_excel(writer, sheet_name="changes_summary", index=False)
        new_df.to_excel(writer, sheet_name="new_plans", index=False)
        updated_df.to_excel(writer, sheet_name="updated_plans", index=False)
        removed_df.to_excel(writer, sheet_name="removed_plans", index=False)
    style_workbook(delta_path, "new_plans", "updated_plans")

    shutil.copyfile(output_path, PREVIOUS_SNAPSHOT)

    logger.info("Exported %d grants to %s", len(summary_df), output_path)
    logger.info("Exported delta workbook to %s", delta_path)
    stats_payload = json.dumps({
        "current_count": int(len(summary_df)),
        "new_count": int(len(new_df)),
        "updated_count": int(len(updated_df)),
        "removed_count": int(len(removed_df)),
    }, ensure_ascii=False)
    logger.info("Stats: %s", stats_payload)
    print(stats_payload)


if __name__ == "__main__":
    asyncio.run(main())
