import asyncio
import json
import logging
import os
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from pydantic import BaseModel, Field, HttpUrl, field_validator
from playwright.async_api import BrowserContext, Page, TimeoutError as PlaywrightTimeoutError, async_playwright

try:
    from playwright_stealth import stealth_async as _stealth
except Exception:
    _stealth = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 20

LOCAL_GOV_HOSTS = {
    "台北": "www.gov.taipei",
    "臺北": "www.gov.taipei",
    "新北": "www.ntpc.gov.tw",
    "基隆": "www.klcg.gov.tw",
    "桃園": "www.tycg.gov.tw",
    "新竹市": "www.hccg.gov.tw",
    "新竹縣": "www.hsinchu.gov.tw",
    "苗栗": "www.miaoli.gov.tw",
    "台中": "www.taichung.gov.tw",
    "臺中": "www.taichung.gov.tw",
    "彰化": "www.chcg.gov.tw",
    "南投": "www.nantou.gov.tw",
    "雲林": "www.yunlin.gov.tw",
    "嘉義市": "www.chiayi.gov.tw",
    "嘉義縣": "www.cyhg.gov.tw",
    "台南": "www.tainan.gov.tw",
    "臺南": "www.tainan.gov.tw",
    "高雄": "www.kcg.gov.tw",
    "屏東": "www.pthg.gov.tw",
    "宜蘭": "www.e-land.gov.tw",
    "花蓮": "www.hl.gov.tw",
    "台東": "www.taitung.gov.tw",
    "臺東": "www.taitung.gov.tw",
    "澎湖": "www.penghu.gov.tw",
    "金門": "www.kinmen.gov.tw",
    "連江": "www.matsu.gov.tw",
}

CENTRAL_SOURCE_HINTS = {
    "勞動部": ["www.mol.gov.tw", "wlb.mol.gov.tw"],
    "數位發展部": ["www.moda.gov.tw", "digiplus.adi.gov.tw"],
    "客家委員會": ["www.hakka.gov.tw"],
    "原住民族委員會": ["www.cip.gov.tw"],
    "教育部": ["www.moe.gov.tw"],
    "文化部": ["www.moc.gov.tw"],
    "國家發展委員會": ["www.ndc.gov.tw"],
    "海洋委員會": ["www.oac.gov.tw"],
    "經濟部": ["www.moea.gov.tw", "www.sme.gov.tw"],
    "農業部": ["www.moa.gov.tw"],
    "環境部": ["www.moenv.gov.tw"],
}

BAD_HOST_KEYWORDS = [
    "google.com",
    "bing.com",
    "duckduckgo.com",
    "104.com.tw",
    "facebook.com",
    "instagram.com",
    "youtube.com",
    "x.com",
    "threads.net",
    "line.me",
]

TRACKED_FIELDS = [
    "title",
    "plan_source",
    "eligible_targets",
    "applicable_region",
    "grant_amount",
    "deadline_date",
    "deadline_text",
    "organizer_site_url",
    "official_organizer_site_url",
    "official_url_status",
]


class GrantSummary(BaseModel):
    title: str
    url: HttpUrl
    topics: List[str] = Field(default_factory=list)
    recipients: List[str] = Field(default_factory=list)
    sources: List[str] = Field(default_factory=list)
    amount: Optional[str] = None
    deadline: Optional[str] = None

    @field_validator("deadline", mode="before")
    @classmethod
    def clean_deadline(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class GrantDetail(BaseModel):
    title: str
    plan_source: Optional[str] = None
    eligible_targets: Optional[str] = None
    applicable_region: Optional[str] = None
    grant_amount: Optional[str] = None
    deadline_date: Optional[str] = None
    deadline_text: Optional[str] = None
    topic_1: Optional[str] = None
    topic_2: Optional[str] = None
    topic_3: Optional[str] = None
    topic_4: Optional[str] = None
    topic_5: Optional[str] = None
    organizer_site_url_raw: Optional[str] = None
    organizer_search_query: Optional[str] = None
    organizer_site_url: Optional[str] = None
    organizer_site_domain: Optional[str] = None
    official_organizer_site_url: Optional[str] = None
    official_organizer_domain: Optional[str] = None
    official_url_status: Optional[str] = None
    detail_url: str
    plan_background: Optional[str] = None
    plan_key_points: Optional[str] = None
    application_tips: Optional[str] = None
    raw_text: Optional[str] = None


@dataclass
class Resolution:
    organizer_url: Optional[str]
    organizer_domain: Optional[str]
    official_url: Optional[str]
    official_domain: Optional[str]
    status: str
    search_query: Optional[str]


class SearchResolver:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8"})
        self.cache: Dict[str, Resolution] = {}

    def resolve(self, raw_url: Optional[str], title: str, plan_source: Optional[str], region: Optional[str]) -> Resolution:
        cache_key = json.dumps([raw_url, title, plan_source, region], ensure_ascii=False)
        if cache_key in self.cache:
            return self.cache[cache_key]

        raw_url = (raw_url or "").strip() or None
        if raw_url and self._is_direct_candidate(raw_url):
            validated = self._validate_url(raw_url)
            if validated:
                res = Resolution(validated, self._host(validated), validated, self._host(validated), "direct_non_google", None)
                self.cache[cache_key] = res
                return res

        official_hosts = self._official_hosts(plan_source, region, title)
        query = self._extract_query(raw_url) or self._normalize_title(title)

        for host in official_hosts:
            found = self._search_bing_site(query, host, title)
            if found:
                validated = self._validate_url(found)
                if validated:
                    res = Resolution(validated, self._host(validated), validated, self._host(validated), "bing_site_verified", query)
                    self.cache[cache_key] = res
                    return res

        if raw_url and self._is_direct_candidate(raw_url):
            validated = self._validate_url(raw_url)
            if validated:
                res = Resolution(validated, self._host(validated), validated, self._host(validated), "direct_official", query)
                self.cache[cache_key] = res
                return res

        for host in official_hosts:
            homepage = self._validate_url(f"https://{host}/")
            if homepage:
                res = Resolution(homepage, self._host(homepage), homepage, self._host(homepage), "verified_portal_homepage", query)
                self.cache[cache_key] = res
                return res

        fallback = raw_url or f"https://www.google.com/search?q={quote_plus(query)}"
        res = Resolution(fallback, self._host(fallback), None, None, "bing_title_no_match", query)
        self.cache[cache_key] = res
        return res

    def _official_hosts(self, plan_source: Optional[str], region: Optional[str], title: str) -> List[str]:
        hosts: List[str] = []
        source = (plan_source or "").strip()
        if source in CENTRAL_SOURCE_HINTS:
            hosts.extend(CENTRAL_SOURCE_HINTS[source])
        if source == "縣市政府":
            reg = region or self._extract_region_from_text(title)
            host = LOCAL_GOV_HOSTS.get(reg or "")
            if host:
                hosts.append(host)
        else:
            reg = self._extract_region_from_text(title) or region
            if reg and reg in LOCAL_GOV_HOSTS:
                hosts.append(LOCAL_GOV_HOSTS[reg])
        # unique preserving order
        seen = set()
        ordered = []
        for h in hosts:
            if h not in seen:
                seen.add(h)
                ordered.append(h)
        return ordered

    def _extract_region_from_text(self, text: Optional[str]) -> Optional[str]:
        text = text or ""
        for key in LOCAL_GOV_HOSTS:
            if key in text:
                return key
        return None

    def _extract_query(self, raw_url: Optional[str]) -> Optional[str]:
        if not raw_url:
            return None
        parsed = urlparse(raw_url)
        if "google." in parsed.netloc and parsed.path.startswith("/search"):
            q = parse_qs(parsed.query).get("q", [None])[0]
            return unquote(q) if q else None
        return None

    def _normalize_title(self, title: str) -> str:
        text = re.sub(r"^\d{2,3}年度", "", title)
        text = re.sub(r"^\d{4}", "", text)
        return text.strip()

    def _is_direct_candidate(self, url: str) -> bool:
        host = self._host(url)
        return bool(host) and all(bad not in host for bad in BAD_HOST_KEYWORDS)

    def _host(self, url: str) -> Optional[str]:
        try:
            return urlparse(url).netloc or None
        except Exception:
            return None

    def _validate_url(self, url: str) -> Optional[str]:
        candidates = [url]
        parsed = urlparse(url)
        host = parsed.netloc
        if host and not host.startswith("www."):
            candidates.append(url.replace(f"//{host}", f"//www.{host}", 1))
        elif host and host.startswith("www."):
            naked = host[4:]
            candidates.append(url.replace(f"//{host}", f"//{naked}", 1))
        for candidate in candidates:
            try:
                r = self.session.get(candidate, allow_redirects=True, timeout=REQUEST_TIMEOUT)
                final_host = urlparse(r.url).netloc.lower()
                if r.status_code < 400 and final_host and all(bad not in final_host for bad in BAD_HOST_KEYWORDS):
                    return r.url
            except requests.RequestException:
                continue
        return None

    def _search_bing_site(self, query: str, host: str, title: str) -> Optional[str]:
        queries = [
            f'site:{host} "{title}"',
            f'site:{host} "{query}" 補助',
            f'site:{host} "{query}" 公告',
            f'site:{host} "{query}" PDF',
        ]
        for q in queries:
            try:
                url = f"https://www.bing.com/search?q={quote_plus(q)}"
                resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
                if resp.status_code >= 400:
                    continue
                soup = BeautifulSoup(resp.text, "html.parser")
                for a in soup.select("li.b_algo h2 a"):
                    href = (a.get("href") or "").strip()
                    if not href:
                        continue
                    host_found = self._host(href) or ""
                    if host in host_found or host_found.endswith(host.replace("www.", "")):
                        return href
            except requests.RequestException:
                continue
        return None


def get_crawl_delay(domain: str) -> float:
    robots_url = f"{domain.rstrip('/')}/robots.txt"
    try:
        r = requests.get(robots_url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        text = r.text
    except requests.RequestException:
        return 0.0
    current_agent = None
    crawl_delay = 0.0
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"user-agent:\s*(.*)", line, re.I)
        if m:
            current_agent = m.group(1).strip()
            continue
        if current_agent in ("*", None):
            d = re.match(r"crawl-delay:\s*(\d+)", line, re.I)
            if d:
                crawl_delay = float(d.group(1))
    return crawl_delay


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def extract_deadline_parts(deadline_text: Optional[str], topics: List[str]) -> Tuple[Optional[str], Optional[str]]:
    if not deadline_text:
        return None, None
    date_match = re.search(r"(20\d{2}-\d{2}-\d{2})", deadline_text)
    date_part = date_match.group(1) if date_match else None
    tags = [t for t in topics if t]
    text_part = "｜".join(tags) if tags else re.sub(r"^.*?(20\d{2}-\d{2}-\d{2})", "", deadline_text).strip("｜ ")
    return date_part, text_part or None


def style_workbook(path: Path) -> None:
    wb = load_workbook(path)
    header_fill = PatternFill("solid", fgColor="1F4E78")
    header_font = Font(color="FFFFFF", bold=True)
    for ws in wb.worksheets:
        ws.freeze_panes = "A2"
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")
        for col_cells in ws.columns:
            max_len = 0
            col_letter = col_cells[0].column_letter
            for c in col_cells:
                c.alignment = Alignment(wrap_text=True, vertical="top")
                value = "" if c.value is None else str(c.value)
                max_len = max(max_len, min(len(value), 60))
            ws.column_dimensions[col_letter].width = max(12, min(max_len + 2, 48))
    wb.save(path)


class GrantCrawler:
    def __init__(self, base_url: str = "https://dayseechat.com") -> None:
        self.base_url = base_url.rstrip("/")
        self.listing_url = f"{self.base_url}/explore-grant/"
        self.resolver = SearchResolver()

    async def _new_context(self, p) -> BrowserContext:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=USER_AGENT, locale="zh-TW", extra_http_headers={"Accept-Language": "zh-TW,zh;q=0.9"})
        return context

    async def _apply_stealth(self, page: Page) -> None:
        if _stealth is not None:
            try:
                await _stealth(page)
            except Exception:
                pass

    async def extract_listings(self, page: Page) -> List[GrantSummary]:
        await page.goto(self.listing_url, wait_until="networkidle")
        await self._apply_stealth(page)
        await page.wait_for_selector("a.ts-action-con")
        seen: Dict[str, GrantSummary] = {}
        last_first_title = None
        for page_no in range(1, 30):
            logger.info("Scanning listing page %d", page_no)
            cards = await page.query_selector_all("a.ts-action-con")
            current_titles = []
            for a in cards:
                href = await a.get_attribute("href")
                title = (await a.get_attribute("aria-label")) or await a.inner_text()
                title = clean_text(title)
                if href and title and "/subsidy/grant-" in href and href not in seen:
                    current_titles.append(title)
                    seen[href] = GrantSummary(title=title, url=href)
            if current_titles:
                if last_first_title == current_titles[0]:
                    break
                last_first_title = current_titles[0]
            next_btn = await page.query_selector("a.ts-load-next:not(.ts-btn-disabled)")
            if not next_btn:
                break
            try:
                await next_btn.scroll_into_view_if_needed()
                await page.evaluate("el => el.click()", next_btn)
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(random.uniform(0.8, 1.4))
            except PlaywrightTimeoutError:
                break
        return list(seen.values())

    async def extract_detail(self, context: BrowserContext, summary: GrantSummary) -> GrantDetail:
        page = await context.new_page()
        try:
            logger.info("Fetching detail: %s", summary.url)
            await page.goto(str(summary.url), wait_until="networkidle")
            await self._apply_stealth(page)
            await page.wait_for_selector("body")
            text = clean_text(await page.locator("body").inner_text())
            title = summary.title

            def find_label(label: str) -> Optional[str]:
                patterns = [
                    rf"{label}[：:]\s*([^\n]+)",
                    rf"{label}\s*([^\n]+)",
                ]
                for p in patterns:
                    m = re.search(p, text)
                    if m:
                        return clean_text(m.group(1))
                return None

            plan_source = find_label("計畫來源")
            eligible = find_label("補助對象")
            region = find_label("適用地區")
            amount = find_label("補助金額")
            deadline_line = find_label("截止日期")

            topics: List[str] = []
            for tag in ["婦女", "產業發展", "青年", "創新創業", "農漁村議題", "客家", "文化藝文", "原住民", "生態環境", "社區議題"]:
                if tag in text and tag not in topics:
                    topics.append(tag)
            deadline_date, deadline_text = extract_deadline_parts(deadline_line, topics)
            topic_cols = topics[:5] + [None] * (5 - len(topics[:5]))

            raw_link = None
            for a in await page.query_selector_all("a[href]"):
                href = (await a.get_attribute("href") or "").strip()
                label = clean_text(await a.inner_text())
                if "申請文件" in label or "主辦單位" in label or "前往主辦單位網站" in label:
                    raw_link = href
                    break
            if not raw_link:
                for a in await page.query_selector_all("a[href]"):
                    href = (await a.get_attribute("href") or "").strip()
                    if "google.com/search?q=" in href:
                        raw_link = href
                        break

            resolution = self.resolver.resolve(raw_link, title, plan_source, region)

            return GrantDetail(
                title=title,
                plan_source=plan_source,
                eligible_targets=eligible,
                applicable_region=region,
                grant_amount=amount,
                deadline_date=deadline_date,
                deadline_text=deadline_text,
                topic_1=topic_cols[0],
                topic_2=topic_cols[1],
                topic_3=topic_cols[2],
                topic_4=topic_cols[3],
                topic_5=topic_cols[4],
                organizer_site_url_raw=raw_link,
                organizer_search_query=resolution.search_query,
                organizer_site_url=resolution.organizer_url,
                organizer_site_domain=resolution.organizer_domain,
                official_organizer_site_url=resolution.official_url,
                official_organizer_domain=resolution.official_domain,
                official_url_status=resolution.status,
                detail_url=str(summary.url),
                raw_text=text,
            )
        finally:
            await page.close()

    async def run(self) -> List[GrantDetail]:
        crawl_delay = get_crawl_delay(self.base_url)
        async with async_playwright() as p:
            context = await self._new_context(p)
            page = await context.new_page()
            listings = await self.extract_listings(page)
            await page.close()
            await context.close()
            logger.info("Found %d grants", len(listings))
            details: List[GrantDetail] = []
            for item in listings:
                context = await self._new_context(p)
                detail = await self.extract_detail(context, item)
                details.append(detail)
                await context.close()
                await asyncio.sleep(crawl_delay + random.uniform(0.1, 0.5))
        return details


def compute_changes(current: pd.DataFrame, previous: Optional[pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if previous is None or previous.empty:
        summary = pd.DataFrame([{"metric": "run_type", "value": "first_run"}, {"metric": "new_count", "value": len(current)}, {"metric": "updated_count", "value": 0}, {"metric": "removed_count", "value": 0}])
        return summary, current.copy(), pd.DataFrame(columns=current.columns.tolist() + ["changed_fields"]), pd.DataFrame(columns=current.columns)

    curr = current.copy().set_index("detail_url", drop=False)
    prev = previous.copy().set_index("detail_url", drop=False)

    new_urls = [u for u in curr.index if u not in prev.index]
    removed_urls = [u for u in prev.index if u not in curr.index]
    common_urls = [u for u in curr.index if u in prev.index]

    updated_rows = []
    for u in common_urls:
        changed = []
        for field in TRACKED_FIELDS:
            a = "" if pd.isna(curr.at[u, field]) else str(curr.at[u, field])
            b = "" if pd.isna(prev.at[u, field]) else str(prev.at[u, field])
            if a != b:
                changed.append(field)
        if changed:
            row = curr.loc[u].to_dict()
            row["changed_fields"] = " | ".join(changed)
            updated_rows.append(row)

    new_df = curr.loc[new_urls].reset_index(drop=True) if new_urls else pd.DataFrame(columns=current.columns)
    removed_df = prev.loc[removed_urls].reset_index(drop=True) if removed_urls else pd.DataFrame(columns=current.columns)
    updated_df = pd.DataFrame(updated_rows)
    summary = pd.DataFrame(
        [
            {"metric": "run_type", "value": "delta_run"},
            {"metric": "current_count", "value": len(current)},
            {"metric": "previous_count", "value": len(previous)},
            {"metric": "new_count", "value": len(new_df)},
            {"metric": "updated_count", "value": len(updated_df)},
            {"metric": "removed_count", "value": len(removed_df)},
        ]
    )
    return summary, new_df, updated_df, removed_df


def write_outputs(details: List[GrantDetail], output_path: Path, delta_output_path: Path, previous_xlsx: Optional[Path]) -> Dict[str, int]:
    summary_rows = [d.model_dump() for d in details]
    summary_df = pd.DataFrame(summary_rows)
    detail_df = summary_df.copy()

    previous_summary = None
    if previous_xlsx and previous_xlsx.exists():
        try:
            previous_summary = pd.read_excel(previous_xlsx, sheet_name="grants_summary")
        except Exception:
            previous_summary = None

    changes_summary, new_df, updated_df, removed_df = compute_changes(summary_df, previous_summary)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="grants_summary", index=False)
        detail_df.to_excel(writer, sheet_name="grants_detail", index=False)
        changes_summary.to_excel(writer, sheet_name="weekly_changes_summary", index=False)
        new_df.to_excel(writer, sheet_name="weekly_new_plans", index=False)
        updated_df.to_excel(writer, sheet_name="weekly_updated_plans", index=False)
        removed_df.to_excel(writer, sheet_name="weekly_removed_plans", index=False)
    style_workbook(output_path)

    with pd.ExcelWriter(delta_output_path, engine="openpyxl") as writer:
        changes_summary.to_excel(writer, sheet_name="changes_summary", index=False)
        new_df.to_excel(writer, sheet_name="new_plans", index=False)
        updated_df.to_excel(writer, sheet_name="updated_plans", index=False)
        removed_df.to_excel(writer, sheet_name="removed_plans", index=False)
    style_workbook(delta_output_path)

    return {
        "current_count": len(summary_df),
        "new_count": len(new_df),
        "updated_count": len(updated_df),
        "removed_count": len(removed_df),
    }


async def async_main(output_path: Path, delta_output_path: Path, previous_xlsx: Optional[Path]) -> Dict[str, int]:
    crawler = GrantCrawler()
    details = await crawler.run()
    stats = write_outputs(details, output_path, delta_output_path, previous_xlsx)
    logger.info("Exported %d records to %s", stats["current_count"], output_path)
    logger.info("Delta workbook written to %s", delta_output_path)
    return stats


def main() -> None:
    output_path = Path(os.getenv("OUTPUT_XLSX", "daysee_grants.xlsx"))
    delta_output_path = Path(os.getenv("DELTA_XLSX", "daysee_grants_delta_only.xlsx"))
    previous = os.getenv("PREVIOUS_XLSX")
    previous_xlsx = Path(previous) if previous else None
    stats = asyncio.run(async_main(output_path, delta_output_path, previous_xlsx))
    print(json.dumps(stats, ensure_ascii=False))


if __name__ == "__main__":
    main()
