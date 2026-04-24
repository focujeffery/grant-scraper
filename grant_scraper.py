import asyncio
import logging
import re
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

import pandas as pd
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font
from playwright.async_api import async_playwright, BrowserContext, Page

try:
    from playwright_stealth import stealth_async as _stealth_async
except Exception:
    _stealth_async = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

BASE_URL = "https://dayseechat.com"
LISTING_URL = f"{BASE_URL}/explore-grant/"
OUTPUT_FILE = "daysee_grants.xlsx"
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
GOOGLE_HOSTS = {"google.com", "www.google.com", "google.com.tw", "www.google.com.tw"}
OFFICIAL_DOMAIN_HINTS = (
    ".gov.tw",
    ".gov",
    ".mil.tw",
    ".edu.tw",
)
OFFICIAL_DOMAIN_KEYWORDS = (
    "gov.tw",
    "gov",
    "moi",
    "moea",
    "mohw",
    "mola",
    "ndc",
    "ares",
    "gov.taipei",
    "taichung.gov.tw",
    "tainan.gov.tw",
    "kcg.gov.tw",
    "newtaipei.gov.tw",
)
SEARCH_RESULT_EXCLUDE = (
    "google.com",
    "youtube.com",
    "facebook.com",
    "instagram.com",
    "threads.net",
    "dayseechat.com",
    "104.com.tw",
    "1111.com.tw",
)


@dataclass
class GrantRow:
    title: str = ""
    detail_url: str = ""
    plan_source: str = ""
    eligible_targets: str = ""
    applicable_region: str = ""
    grant_amount: str = ""
    organizer_site_url: str = ""
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


def normalize_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    value = value.replace("\u3000", " ").replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def split_lines(text: str) -> List[str]:
    lines = [normalize_text(line) for line in text.splitlines()]
    return [line for line in lines if line]


def get_domain(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def is_google_search_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc.lower() in GOOGLE_HOSTS and parsed.path in {"/search", "/url"}


def extract_query_from_google_url(url: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    if "q" in params and params["q"]:
        return normalize_text(params["q"][0])
    return ""


def is_official_domain(domain: str) -> bool:
    domain = domain.lower().strip()
    if not domain:
        return False
    if any(excluded in domain for excluded in SEARCH_RESULT_EXCLUDE):
        return False
    if domain.endswith(OFFICIAL_DOMAIN_HINTS):
        return True
    return any(keyword in domain for keyword in OFFICIAL_DOMAIN_KEYWORDS)


def score_candidate_url(url: str, title: str, plan_source: str) -> int:
    domain = get_domain(url)
    score = 0
    if is_official_domain(domain):
        score += 100
    if url.startswith("https://"):
        score += 5
    lower_url = url.lower()
    lower_title = title.lower()
    lower_source = plan_source.lower()
    if DATE_RE.search(lower_title):
        score += 3
    title_tokens = [t for t in re.split(r"[^\w\u4e00-\u9fff]+", lower_title) if len(t) >= 2][:8]
    for token in title_tokens:
        if token and token in lower_url:
            score += 6
    if lower_source and lower_source in lower_url:
        score += 12
    if "subsidy" in lower_url or "grant" in lower_url or "plan" in lower_url or "project" in lower_url:
        score += 4
    return score


async def get_crawl_delay(domain: str) -> float:
    url = f"{domain.rstrip('/')}/robots.txt"
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            logger.info("Fetching robots.txt from %s", url)
            await page.goto(url)
            text = await page.content()
        finally:
            await browser.close()

    crawl_delay = 0.0
    current_agent = None
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m_agent = re.match(r"user-agent:\s*(.*)", line, re.I)
        if m_agent:
            current_agent = m_agent.group(1).strip()
            continue
        if current_agent in ("*", None):
            m_delay = re.match(r"crawl-delay:\s*(\d+)", line, re.I)
            if m_delay:
                crawl_delay = float(m_delay.group(1))
                break
    logger.info("Crawl delay parsed: %.1f seconds", crawl_delay)
    return crawl_delay


class GrantCrawler:
    def __init__(self) -> None:
        self.base_url = BASE_URL
        self.listing_url = LISTING_URL
        self.search_cache: Dict[str, Tuple[str, str, str]] = {}

    async def _apply_stealth(self, page: Page) -> None:
        if _stealth_async is None:
            return
        try:
            await _stealth_async(page)
        except Exception as exc:
            logger.warning("Stealth plugin unavailable or incompatible: %s", exc)

    async def _new_context(self, playwright) -> BrowserContext:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(
            locale="zh-TW",
            extra_http_headers={"Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8"},
        )
        return context

    async def _extract_current_page_cards(self, page: Page) -> List[Dict[str, str]]:
        cards = await page.eval_on_selector_all(
            'a[href*="/subsidy/grant-"]',
            r"""
            (anchors) => anchors
              .map((a) => ({
                href: a.href,
                title: (a.textContent || '').trim()
              }))
              .filter((x) => /\/subsidy\/grant-\d+\/?$/.test(x.href) && x.title.length > 0)
            """,
        )
        deduped: Dict[str, Dict[str, str]] = {}
        for card in cards:
            href = card["href"].strip()
            title = (card["title"] or "").strip()
            if href not in deduped and title:
                deduped[href] = {"href": href, "title": normalize_text(title)}
        return list(deduped.values())

    async def _expected_total(self, page: Page) -> Optional[int]:
        body_text = await page.locator("body").inner_text()
        match = re.search(r"(\d+)\s+out\s+of\s+(\d+)\s+results", body_text, re.I)
        if match:
            return int(match.group(2))
        return None

    async def _go_next_page(self, page: Page) -> bool:
        next_btn = page.locator("a.ts-load-next").first
        if await next_btn.count() == 0:
            return False
        classes = (await next_btn.get_attribute("class")) or ""
        if "ts-btn-disabled" in classes:
            return False

        before = {c["href"] for c in await self._extract_current_page_cards(page)}
        await next_btn.evaluate("el => el.click()")
        await page.wait_for_timeout(1200)

        for _ in range(12):
            await page.wait_for_timeout(350)
            after = {c["href"] for c in await self._extract_current_page_cards(page)}
            if after and after != before:
                return True

        logger.info("Pagination did not advance; treating this as the last page.")
        return False

    async def extract_listing(self, page: Page) -> List[Dict[str, str]]:
        await page.goto(self.listing_url, wait_until="networkidle")
        await self._apply_stealth(page)
        await page.wait_for_selector('a[href*="/subsidy/grant-"]')

        total_expected = await self._expected_total(page)
        all_cards: Dict[str, Dict[str, str]] = {}
        page_index = 1
        max_pages = 30

        while page_index <= max_pages:
            logger.info("Scanning listing page %d", page_index)
            cards = await self._extract_current_page_cards(page)
            for card in cards:
                all_cards.setdefault(card["href"], card)

            if total_expected and len(all_cards) >= total_expected:
                break

            advanced = await self._go_next_page(page)
            if not advanced:
                break
            page_index += 1

        results = list(all_cards.values())
        logger.info("Found %d grants", len(results))
        return results

    def _trim_to_main_content(self, lines: List[str]) -> List[str]:
        cut_markers = ["返回主頁", "你可能也會喜歡這些資訊", "無符合結果", "上一頁 下一頁"]
        for idx, line in enumerate(lines):
            if any(marker in line for marker in cut_markers):
                return lines[:idx]
        return lines

    def _canonical_label(self, value: str) -> str:
        clean = value.replace("：", ":")
        clean = normalize_text(clean.rstrip(":"))
        return clean

    def _parse_meta(self, meta_lines: List[str]) -> Dict[str, str]:
        label_map = {
            "計畫來源": "plan_source",
            "補助對象": "eligible_targets",
            "適用地區": "applicable_region",
            "補助金額": "grant_amount",
            "截止日期": "deadline_date",
        }
        parsed: Dict[str, str] = {v: "" for v in label_map.values()}
        parsed["deadline_text"] = ""
        parsed["topics"] = ""

        i = 0
        while i < len(meta_lines):
            label = self._canonical_label(meta_lines[i])
            if label not in label_map:
                i += 1
                continue

            j = i + 1
            values: List[str] = []
            while j < len(meta_lines):
                next_label = self._canonical_label(meta_lines[j])
                current_line = meta_lines[j]
                if next_label in label_map:
                    break
                if "本資訊為AI生成工具" in current_line:
                    break
                if current_line.startswith("申請文件"):
                    j += 1
                    continue
                values.append(current_line)
                j += 1

            field_name = label_map[label]
            if field_name == "deadline_date":
                date_value = ""
                text_values: List[str] = []
                for v in values:
                    if DATE_RE.fullmatch(v) and not date_value:
                        date_value = v
                    else:
                        text_values.append(v)
                parsed["deadline_date"] = date_value
                parsed["deadline_text"] = "｜".join(text_values)
                parsed["topics"] = "｜".join(text_values)
            else:
                parsed[field_name] = "｜".join(values)
            i = j

        return parsed

    def _extract_sections(self, content_lines: List[str]) -> Dict[str, List[str]]:
        sections = {"計畫背景": [], "計畫重點": [], "撰寫技巧": []}
        current_section = None
        for line in content_lines:
            if line in sections:
                current_section = line
                continue
            if current_section:
                sections[current_section].append(line)
        return sections

    async def resolve_official_url(self, context: BrowserContext, title: str, organizer_site_url: str, plan_source: str) -> Tuple[str, str, str]:
        organizer_site_url = normalize_text(organizer_site_url)
        if not organizer_site_url:
            return "", "", "empty"

        existing_domain = get_domain(organizer_site_url)
        if organizer_site_url.startswith("http") and not is_google_search_url(organizer_site_url):
            if is_official_domain(existing_domain):
                return organizer_site_url, existing_domain, "direct_official"
            return organizer_site_url, existing_domain, "direct_non_google"

        query = extract_query_from_google_url(organizer_site_url)
        if not query:
            query = normalize_text(title)
        if not query:
            return "", "", "no_query"

        cache_key = f"{query}|{plan_source}"
        if cache_key in self.search_cache:
            return self.search_cache[cache_key]

        page = await context.new_page()
        try:
            await self._apply_stealth(page)
            advanced_query = f'{query} site:gov.tw OR site:gov'
            search_url = f"https://www.google.com/search?q={quote_plus(advanced_query)}&hl=zh-TW"
            logger.info("Resolving official URL via Google: %s", query)
            await page.goto(search_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(1800)

            consent_buttons = [
                "button:has-text('全部接受')",
                "button:has-text('接受全部')",
                "button:has-text('I agree')",
                "button:has-text('Accept all')",
            ]
            for selector in consent_buttons:
                try:
                    locator = page.locator(selector).first
                    if await locator.count() > 0:
                        await locator.click(timeout=800)
                        await page.wait_for_timeout(1200)
                        break
                except Exception:
                    pass

            candidates = await page.eval_on_selector_all(
                "a[href]",
                r"""
                (anchors) => anchors.map(a => ({
                    href: a.href,
                    text: (a.textContent || '').trim()
                }))
                """,
            )

            best_url = ""
            best_domain = ""
            best_status = "google_no_match"
            best_score = -10**9
            for item in candidates:
                href = (item.get("href") or "").strip()
                if not href:
                    continue
                if href.startswith("/url?"):
                    href = "https://www.google.com" + href
                parsed = urlparse(href)
                if parsed.netloc.lower() in GOOGLE_HOSTS and parsed.path == "/url":
                    qs = parse_qs(parsed.query)
                    href = (qs.get("q") or [""])[0]
                href = normalize_text(href)
                if not href.startswith("http"):
                    continue
                domain = get_domain(href)
                if not domain or any(excluded in domain for excluded in SEARCH_RESULT_EXCLUDE):
                    continue
                score = score_candidate_url(href, title, plan_source)
                if score > best_score:
                    best_url = href
                    best_domain = domain
                    best_score = score
                    best_status = "google_official_match" if is_official_domain(domain) else "google_best_effort"

            result = (best_url, best_domain, best_status)
            self.search_cache[cache_key] = result
            return result
        except Exception as exc:
            logger.warning("Google official URL resolution failed for %s: %s", query, exc)
            result = (organizer_site_url, existing_domain, "google_lookup_failed")
            self.search_cache[cache_key] = result
            return result
        finally:
            await page.close()

    def _parse_detail_html(self, url: str, html: str) -> GrantRow:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        for node in soup.find_all(style=True):
            style_value = (node.get("style") or "").replace(" ", "").lower()
            if "display:none" in style_value:
                node.decompose()

        organizer_site_url = ""
        application_note = ""
        for a in soup.find_all("a", href=True):
            txt = normalize_text(a.get_text(" ", strip=True))
            href = normalize_text(a.get("href", ""))
            if "申請文件" in txt or "主辦單位網站" in txt:
                organizer_site_url = urljoin(url, href)
                application_note = txt
                break

        lines = split_lines(soup.get_text("\n"))
        lines = self._trim_to_main_content(lines)
        title = normalize_text(lines[0] if lines else "")
        title = re.sub(r"\s*-\s*小社區大事件\s*$", "", title)

        disclaimer_idx = next((i for i, x in enumerate(lines) if "本資訊為AI生成工具" in x), len(lines))
        intro_idx = next((i for i, x in enumerate(lines) if "計畫簡介" in x), disclaimer_idx)

        meta_lines = lines[1:disclaimer_idx]
        content_lines = lines[intro_idx + 1 :] if intro_idx < len(lines) else []

        parsed = self._parse_meta(meta_lines)
        topic_candidates = [normalize_text(x) for x in parsed.get("topics", "").split("｜") if normalize_text(x)]
        topic_candidates = list(dict.fromkeys(topic_candidates))[:5]

        sections = self._extract_sections(content_lines)
        key_points: List[str] = []
        for item in sections["計畫重點"]:
            cleaned = item.lstrip("•・- ").strip()
            if cleaned:
                key_points.append(cleaned)
        key_points = key_points[:5]

        background = "\n".join(sections["計畫背景"]).strip()
        writing_tips = "\n".join(sections["撰寫技巧"]).strip()
        raw_text = "\n".join(lines).strip()

        row = GrantRow(
            title=title,
            detail_url=url,
            plan_source=parsed.get("plan_source", ""),
            eligible_targets=parsed.get("eligible_targets", ""),
            applicable_region=parsed.get("applicable_region", ""),
            grant_amount=parsed.get("grant_amount", ""),
            organizer_site_url=organizer_site_url,
            application_note=application_note,
            deadline_date=parsed.get("deadline_date", ""),
            deadline_text=parsed.get("deadline_text", ""),
            background=background,
            writing_tips=writing_tips,
            raw_text=raw_text,
        )

        for index, topic in enumerate(topic_candidates, start=1):
            setattr(row, f"topic_{index}", topic)
        for index, point in enumerate(key_points, start=1):
            setattr(row, f"key_point_{index}", point)

        return row

    async def extract_detail(self, context: BrowserContext, summary: Dict[str, str]) -> GrantRow:
        page = await context.new_page()
        try:
            logger.info("Fetching detail: %s", summary["href"])
            await page.goto(summary["href"], wait_until="networkidle")
            await self._apply_stealth(page)
            await page.wait_for_selector("h1, h2, h3")
            html = await page.content()
            row = self._parse_detail_html(summary["href"], html)
            if not row.title:
                row.title = re.sub(r"\s*-\s*小社區大事件\s*$", "", summary["title"])
            official_url, official_domain, status = await self.resolve_official_url(
                context=context,
                title=row.title or summary["title"],
                organizer_site_url=row.organizer_site_url,
                plan_source=row.plan_source,
            )
            row.official_organizer_site_url = official_url
            row.official_organizer_domain = official_domain
            row.official_url_status = status
            return row
        finally:
            await page.close()

    async def run(self) -> List[GrantRow]:
        crawl_delay = await get_crawl_delay(self.base_url)
        async with async_playwright() as playwright:
            listing_context = await self._new_context(playwright)
            listing_page = await listing_context.new_page()
            listings = await self.extract_listing(listing_page)
            await listing_page.close()
            await listing_context.close()

            output: List[GrantRow] = []
            for idx, summary in enumerate(listings, start=1):
                detail_context = await self._new_context(playwright)
                row = await self.extract_detail(detail_context, summary)
                await detail_context.close()
                output.append(row)
                await asyncio.sleep(max(crawl_delay, 0.4))
                if idx % 10 == 0:
                    logger.info("Processed %d/%d detail pages", idx, len(listings))
            return output


def save_to_excel(rows: List[GrantRow], output_path: str = OUTPUT_FILE) -> None:
    structured_rows = [asdict(r) for r in rows]
    df = pd.DataFrame(structured_rows)

    ordered_cols = [
        "title",
        "detail_url",
        "plan_source",
        "eligible_targets",
        "applicable_region",
        "grant_amount",
        "organizer_site_url",
        "official_organizer_site_url",
        "official_organizer_domain",
        "official_url_status",
        "application_note",
        "deadline_date",
        "deadline_text",
        "topic_1",
        "topic_2",
        "topic_3",
        "topic_4",
        "topic_5",
        "background",
        "key_point_1",
        "key_point_2",
        "key_point_3",
        "key_point_4",
        "key_point_5",
        "writing_tips",
        "raw_text",
    ]
    df = df.reindex(columns=ordered_cols)

    summary_df = df[
        [
            "title",
            "plan_source",
            "eligible_targets",
            "applicable_region",
            "grant_amount",
            "deadline_date",
            "deadline_text",
            "topic_1",
            "topic_2",
            "topic_3",
            "topic_4",
            "topic_5",
            "organizer_site_url",
            "official_organizer_site_url",
            "official_organizer_domain",
            "official_url_status",
            "detail_url",
        ]
    ].copy()

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="grants_summary", index=False)
        df.to_excel(writer, sheet_name="grants_detail", index=False)

    wb = load_workbook(output_path)
    for sheet_name in ["grants_summary", "grants_detail"]:
        ws = wb[sheet_name]
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        for cell in ws[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center", vertical="center")

        for col_cells in ws.columns:
            letter = col_cells[0].column_letter
            header = str(col_cells[0].value or "")
            if header in {"title", "detail_url", "organizer_site_url", "official_organizer_site_url", "background", "writing_tips", "raw_text"}:
                ws.column_dimensions[letter].width = 42 if "url" in header else 36
            elif header in {"eligible_targets", "deadline_text", "plan_source"}:
                ws.column_dimensions[letter].width = 24
            else:
                ws.column_dimensions[letter].width = 16

        for row in ws.iter_rows(min_row=2):
            for cell in row:
                cell.alignment = Alignment(vertical="top", wrap_text=True)

    wb.save(output_path)
    logger.info("Exported %d records to %s", len(rows), output_path)


async def main() -> None:
    crawler = GrantCrawler()
    rows = await crawler.run()
    save_to_excel(rows, OUTPUT_FILE)


if __name__ == "__main__":
    asyncio.run(main())
