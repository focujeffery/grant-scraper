import asyncio
import logging
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urljoin

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


@dataclass
class GrantRow:
    title: str = ""
    detail_url: str = ""
    plan_source: str = ""
    eligible_targets: str = ""
    applicable_region: str = ""
    grant_amount: str = ""
    application_url: str = ""
    application_note: str = ""
    deadline: str = ""
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
    value = value.replace("\u3000", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def split_lines(text: str) -> List[str]:
    lines = [normalize_text(line) for line in text.splitlines()]
    return [line for line in lines if line]


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
        deduped = {}
        for card in cards:
            href = card["href"].strip()
            title = normalize_text(card["title"])
            if href not in deduped and title:
                deduped[href] = {"href": href, "title": title}
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

    def _parse_detail_html(self, url: str, html: str) -> GrantRow:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        for node in soup.find_all(style=True):
            style_value = (node.get("style") or "").replace(" ", "").lower()
            if "display:none" in style_value:
                node.decompose()

        application_url = ""
        application_note = ""
        for a in soup.find_all("a", href=True):
            txt = normalize_text(a.get_text(" ", strip=True))
            if "申請文件" in txt or "主辦單位網站" in txt:
                application_url = urljoin(url, a["href"])
                application_note = txt
                break

        lines = split_lines(soup.get_text("\n"))
        lines = self._trim_to_main_content(lines)
        title = lines[0] if lines else ""

        intro_idx = next((i for i, x in enumerate(lines) if "計畫簡介" in x), len(lines))
        meta_lines = lines[1:intro_idx]
        content_lines = lines[intro_idx + 1 :] if intro_idx < len(lines) else []

        label_map = {
            "計畫來源": "plan_source",
            "補助對象": "eligible_targets",
            "適用地區": "applicable_region",
            "補助金額": "grant_amount",
            "截止日期": "deadline",
        }

        def canonical_label(value: str) -> str:
            clean = value.replace("：", ":")
            clean = normalize_text(clean.rstrip(":"))
            return clean

        parsed: Dict[str, str] = {v: "" for v in label_map.values()}
        used_indexes = set()
        i = 0
        while i < len(meta_lines):
            current = canonical_label(meta_lines[i])
            if current in label_map:
                field_name = label_map[current]
                j = i + 1
                values = []
                while j < len(meta_lines):
                    next_label = canonical_label(meta_lines[j])
                    if next_label in label_map:
                        break
                    if "本資訊為AI生成工具" not in meta_lines[j] and not meta_lines[j].startswith("申請文件"):
                        values.append(meta_lines[j])
                    used_indexes.add(j)
                    j += 1
                parsed[field_name] = "｜".join(values)
                used_indexes.add(i)
                i = j
            else:
                i += 1

        topic_candidates = []
        for idx, line in enumerate(meta_lines):
            if idx in used_indexes:
                continue
            if "本資訊為AI生成工具" in line:
                continue
            if line.startswith("申請文件"):
                continue
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", line):
                continue
            topic_candidates.append(line)
        topic_candidates = list(dict.fromkeys(topic_candidates))[:5]

        sections = {"計畫背景": [], "計畫重點": [], "撰寫技巧": []}
        current_section = None
        for line in content_lines:
            if line in sections:
                current_section = line
                continue
            if current_section:
                sections[current_section].append(line)

        key_points = []
        for item in sections["計畫重點"]:
            text = item.lstrip("•・- ").strip()
            if text:
                key_points.append(text)
        key_points = key_points[:5]

        background = "\n".join(sections["計畫背景"]).strip()
        writing_tips = "\n".join(sections["撰寫技巧"]).strip()
        raw_text = "\n".join(lines).strip()

        row = GrantRow(
            title=title,
            detail_url=url,
            plan_source=parsed["plan_source"],
            eligible_targets=parsed["eligible_targets"],
            applicable_region=parsed["applicable_region"],
            grant_amount=parsed["grant_amount"],
            application_url=application_url,
            application_note=application_note,
            deadline=parsed["deadline"],
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
                row.title = summary["title"]
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
                if crawl_delay > 0:
                    await asyncio.sleep(crawl_delay)
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
        "application_url",
        "application_note",
        "deadline",
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

    summary_df = df[[
        "title",
        "plan_source",
        "eligible_targets",
        "applicable_region",
        "grant_amount",
        "deadline",
        "topic_1",
        "topic_2",
        "topic_3",
        "topic_4",
        "topic_5",
        "application_url",
        "detail_url",
    ]].copy()

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

        width_map = {
            "A": 28, "B": 18, "C": 20, "D": 18, "E": 14, "F": 12, "G": 12,
            "H": 18, "I": 12, "J": 14, "K": 14, "L": 14, "M": 14, "N": 36,
            "O": 32, "P": 32, "Q": 32, "R": 32, "S": 32, "T": 42, "U": 56,
        }
        for col, width in width_map.items():
            ws.column_dimensions[col].width = width

        for row in ws.iter_rows(min_row=2):
            for cell in row:
                cell.alignment = Alignment(vertical="top", wrap_text=True)
        for row_idx in range(2, ws.max_row + 1):
            ws.row_dimensions[row_idx].height = 48 if sheet_name == "grants_detail" else 24

    wb.save(output_path)
    logger.info("Exported %d records to %s", len(rows), output_path)


async def main() -> None:
    crawler = GrantCrawler()
    rows = await crawler.run()
    save_to_excel(rows, OUTPUT_FILE)


if __name__ == "__main__":
    asyncio.run(main())
