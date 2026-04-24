import asyncio
import logging
import re
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

import pandas as pd
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font
from playwright.async_api import async_playwright, BrowserContext, Page, TimeoutError as PlaywrightTimeoutError

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
BING_HOSTS = {"bing.com", "www.bing.com"}
DDG_HOSTS = {"duckduckgo.com", "www.duckduckgo.com", "html.duckduckgo.com"}
SEARCH_EXCLUDE = {
    "google.com", "www.google.com", "google.com.tw", "www.google.com.tw",
    "bing.com", "www.bing.com",
    "duckduckgo.com", "www.duckduckgo.com", "html.duckduckgo.com",
    "facebook.com", "www.facebook.com",
    "instagram.com", "www.instagram.com",
    "threads.net", "www.threads.net",
    "youtube.com", "www.youtube.com",
    "dayseechat.com", "www.dayseechat.com",
}
OFFICIAL_DOMAIN_HINTS = (
    ".gov.tw", ".gov", "gov.taipei", "gov.kaohsiung", "gov.taichung", "gov.tainan",
)
OFFICIAL_KEYWORDS = (
    "gov.tw", "gov", "mol.gov.tw", "moea.gov.tw", "moc.gov.tw", "nat.gov.tw",
    "taipei.gov.tw", "newtaipei.gov.tw", "taichung.gov.tw", "tainan.gov.tw", "kcg.gov.tw",
    "hl.gov.tw", "cyhg.gov.tw", "nantou.gov.tw", "yunlin.gov.tw", "ptcg.gov.tw",
)
URL_SCORE_HINTS = ("grant", "subsidy", "project", "plan", "apply", "application", "news_content", "article")


SOURCE_DOMAIN_MAP = {
    "勞動部": ["mol.gov.tw", "wda.gov.tw", "ws.wda.gov.tw"],
    "經濟部": ["moea.gov.tw", "sme.gov.tw", "industry.gov.tw", "aoc.moea.gov.tw"],
    "數位發展部": ["moda.gov.tw", "adi.gov.tw", "daas.moda.gov.tw"],
    "文化部": ["moc.gov.tw", "grants.moc.gov.tw"],
    "教育部": ["moe.gov.tw", "edu.tw"],
    "衛生福利部": ["mohw.gov.tw", "hpa.gov.tw"],
    "客家委員會": ["hakka.gov.tw"],
    "原住民族委員會": ["cip.gov.tw"],
    "海洋委員會": ["oac.gov.tw", "oca.gov.tw"],
    "國家發展委員會": ["ndc.gov.tw"],
    "農業部": ["moa.gov.tw", "afa.gov.tw"],
    "環境部": ["moenv.gov.tw", "epa.gov.tw"],
}

REGION_DOMAIN_MAP = {
    "台北": ["taipei.gov.tw"],
    "臺北": ["taipei.gov.tw"],
    "新北": ["ntpc.gov.tw"],
    "桃園": ["tycg.gov.tw"],
    "台中": ["taichung.gov.tw"],
    "臺中": ["taichung.gov.tw"],
    "台南": ["tainan.gov.tw"],
    "臺南": ["tainan.gov.tw"],
    "高雄": ["kcg.gov.tw"],
    "基隆": ["klcg.gov.tw"],
    "新竹": ["hccg.gov.tw", "hsinchu.gov.tw"],
    "彰化": ["changhua.gov.tw"],
    "雲林": ["yunlin.gov.tw"],
    "南投": ["nantou.gov.tw"],
    "台東": ["taitung.gov.tw"],
    "臺東": ["taitung.gov.tw"],
    "連江": ["matsu.gov.tw"],
}


@dataclass
class GrantRow:
    title: str = ""
    detail_url: str = ""
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
        return normalize_text(unquote(params["q"][0]))
    return ""


def is_official_domain(domain: str) -> bool:
    domain = (domain or "").lower().strip()
    if not domain:
        return False
    if domain in SEARCH_EXCLUDE:
        return False
    if any(domain.endswith(hint) for hint in OFFICIAL_DOMAIN_HINTS):
        return True
    return any(keyword in domain for keyword in OFFICIAL_KEYWORDS)


def tokenize(text: str) -> List[str]:
    tokens = re.findall(r"[A-Za-z0-9\u4e00-\u9fff]{2,}", text.lower())
    stop = {"年度", "計畫", "補助", "申請", "推動", "政府", "網站", "主辦", "單位", "官方"}
    return [t for t in tokens if t not in stop]


def split_multi_value_text(value: str) -> List[str]:
    value = normalize_text(value)
    if not value:
        return []
    parts = re.split(r"[｜|/、,，\s]+", value)
    return [normalize_text(p) for p in parts if normalize_text(p)]


def collect_hint_domains(plan_source: str, applicable_region: str) -> List[str]:
    domains: List[str] = []
    for key, values in SOURCE_DOMAIN_MAP.items():
        if key and key in (plan_source or ""):
            domains.extend(values)
    for region in split_multi_value_text(applicable_region):
        for key, values in REGION_DOMAIN_MAP.items():
            if key and key in region:
                domains.extend(values)
    deduped: List[str] = []
    seen = set()
    for domain in domains:
        if domain not in seen:
            seen.add(domain)
            deduped.append(domain)
    return deduped


def score_candidate(url: str, title: str, plan_source: str, query: str, hint_domains: Optional[List[str]] = None) -> int:
    domain = get_domain(url)
    if not url.startswith("http"):
        return -10**9
    if domain in SEARCH_EXCLUDE:
        return -10**9

    score = 0
    lower_url = url.lower()
    if url.startswith("https://"):
        score += 5
    if is_official_domain(domain):
        score += 200
    if any(h in lower_url for h in URL_SCORE_HINTS):
        score += 8
    if hint_domains:
        for hint in hint_domains:
            if domain.endswith(hint) or hint in lower_url:
                score += 120

    title_tokens = tokenize(title)[:10]
    source_tokens = tokenize(plan_source)[:5]
    query_tokens = tokenize(query)[:10]

    for token in title_tokens:
        if token in lower_url:
            score += 10
    for token in source_tokens:
        if token in lower_url:
            score += 14
    for token in query_tokens:
        if token in lower_url:
            score += 6

    if domain.endswith(".org.tw"):
        score += 10
    if domain.endswith(".edu.tw"):
        score += 4
    if lower_url.endswith(".pdf"):
        score += 3
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
        self.search_cache: Dict[str, Tuple[str, str, str, str, str]] = {}

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
        try:
            await next_btn.evaluate("el => el.click()")
        except Exception:
            try:
                await next_btn.click(timeout=1500, force=True)
            except Exception:
                return False
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

    
    async def _collect_bing_candidates(self, page: Page, query: str) -> List[Tuple[str, str]]:
        url = f"https://www.bing.com/search?q={quote_plus(query)}&setlang=zh-Hant"
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(1200)
        items = await page.eval_on_selector_all(
            "li.b_algo h2 a, li.b_algo a, a[href]",
            r"""
            (anchors) => anchors.map(a => ({
                href: a.href,
                text: (a.textContent || '').trim()
            }))
            """,
        )
        return [
            (normalize_text(h), normalize_text(t))
            for h, t in [(i.get("href") or "", i.get("text") or "") for i in items]
            if normalize_text(h).startswith("http")
        ]

    async def _collect_ddg_candidates(self, page: Page, query: str) -> List[Tuple[str, str]]:
        url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(1200)
        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        results: List[Tuple[str, str]] = []
        for a in soup.select("a.result__a, a[href]"):
            href = normalize_text(a.get("href") or "")
            text = normalize_text(a.get_text(" ", strip=True))
            if not href:
                continue
            if href.startswith("//"):
                href = "https:" + href
            if href.startswith("/l/"):
                parsed = urlparse(href)
                qs = parse_qs(parsed.query)
                href = normalize_text((qs.get("uddg") or [""])[0])
            if href.startswith("http"):
                results.append((href, text))
        return results

    def _choose_best_candidates(
        self,
        candidates: List[Tuple[str, str]],
        title: str,
        plan_source: str,
        applicable_region: str,
        query: str,
    ) -> Tuple[str, str, str, str, str]:
        deduped: Dict[str, Tuple[str, str]] = {}
        for href, text in candidates:
            href = normalize_text(href)
            if not href:
                continue
            domain = get_domain(href)
            if not domain or domain in SEARCH_EXCLUDE:
                continue
            deduped[href] = (href, text)

        best_any = ("", "", -10**9)
        best_official = ("", "", -10**9)
        for href, text in deduped.values():
            domain = get_domain(href)
            score = score_candidate(href, title=title, plan_source=plan_source, query=query, hint_domains=collect_hint_domains(plan_source, applicable_region))
            if score > best_any[2]:
                best_any = (href, domain, score)
            if is_official_domain(domain) and score > best_official[2]:
                best_official = (href, domain, score)

        resolved_url = best_official[0] or best_any[0]
        resolved_domain = best_official[1] or best_any[1]
        official_url = best_official[0]
        official_domain = best_official[1]
        if official_url:
            status = "google_official_match"
        elif resolved_url:
            status = "google_best_effort"
        else:
            status = "google_no_match"
        return resolved_url, resolved_domain, official_url, official_domain, status

    async def resolve_organizer_urls(
        self,
        context: BrowserContext,
        title: str,
        organizer_site_url_raw: str,
        plan_source: str,
        applicable_region: str,
    ) -> Tuple[str, str, str, str, str, str]:
        raw = normalize_text(organizer_site_url_raw)
        if not raw:
            return raw, "", "", "", "empty", ""

        raw_domain = get_domain(raw)
        if raw.startswith("http") and not is_google_search_url(raw):
            status = "direct_official" if is_official_domain(raw_domain) else "direct_non_google"
            official_url = raw if is_official_domain(raw_domain) else ""
            official_domain = raw_domain if official_url else ""
            return raw, raw_domain, official_url, official_domain, status, ""

        query = extract_query_from_google_url(raw) or normalize_text(title)
        if not query:
            return raw, raw_domain, "", "", "no_query", ""

        cache_key = f"{query}|{plan_source}|{applicable_region}"
        if cache_key in self.search_cache:
            resolved_url, resolved_domain, official_url, official_domain, status = self.search_cache[cache_key]
            return resolved_url, resolved_domain, official_url, official_domain, status, query

        hint_domains = collect_hint_domains(plan_source, applicable_region)
        search_queries: List[str] = []
        for hint in hint_domains[:4]:
            search_queries.append(f"{title} site:{hint}")
            if plan_source:
                search_queries.append(f"{title} {plan_source} site:{hint}")
        if plan_source:
            search_queries.append(f"{title} {plan_source} 官方")
            search_queries.append(f"{title} {plan_source}")
        if applicable_region and applicable_region != "不分縣市":
            search_queries.append(f"{title} {applicable_region} 補助")
            search_queries.append(f"{title} {applicable_region}")
        search_queries.append(f"{title} 官方")
        search_queries.append(title)

        seen_queries = set()
        search_queries = [q for q in search_queries if q and not (q in seen_queries or seen_queries.add(q))]

        all_candidates: List[Tuple[str, str]] = []
        page = await context.new_page()
        try:
            await self._apply_stealth(page)
            for q in search_queries:
                logger.info("Resolving organizer URL via stable search: %s", q)
                try:
                    all_candidates.extend(await self._collect_bing_candidates(page, q))
                except Exception as exc:
                    logger.warning("Bing lookup failed for %s: %s", q, exc)
                try:
                    all_candidates.extend(await self._collect_ddg_candidates(page, q))
                except Exception as exc:
                    logger.warning("DuckDuckGo lookup failed for %s: %s", q, exc)

                resolved_url, resolved_domain, official_url, official_domain, status = self._choose_best_candidates(
                    all_candidates,
                    title=title,
                    plan_source=plan_source,
                    applicable_region=applicable_region,
                    query=q,
                )
                if official_url:
                    self.search_cache[cache_key] = (resolved_url, resolved_domain, official_url, official_domain, status)
                    return resolved_url, resolved_domain, official_url, official_domain, status, q

            resolved_url, resolved_domain, official_url, official_domain, status = self._choose_best_candidates(
                all_candidates,
                title=title,
                plan_source=plan_source,
                applicable_region=applicable_region,
                query=query,
            )
            if not resolved_url:
                resolved_url = raw
                resolved_domain = raw_domain
                status = "search_fallback_original"
            elif not official_url:
                status = "search_best_effort"
            self.search_cache[cache_key] = (resolved_url, resolved_domain, official_url, official_domain, status)
            return resolved_url, resolved_domain, official_url, official_domain, status, query
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

        organizer_site_url_raw = ""
        application_note = ""
        for a in soup.find_all("a", href=True):
            txt = normalize_text(a.get_text(" ", strip=True))
            href = normalize_text(a.get("href", ""))
            if "申請文件" in txt or "主辦單位網站" in txt:
                organizer_site_url_raw = urljoin(url, href)
                application_note = txt
                break

        lines = split_lines(soup.get_text("\n"))
        lines = self._trim_to_main_content(lines)
        title = normalize_text(lines[0] if lines else "")
        title = re.sub(r"\s*-\s*小社區大事件\s*$", "", title)

        disclaimer_idx = next((i for i, x in enumerate(lines) if "本資訊為AI生成工具" in x), len(lines))
        intro_idx = next((i for i, x in enumerate(lines) if "計畫簡介" in x), disclaimer_idx)

        meta_lines = lines[1:disclaimer_idx]
        content_lines = lines[intro_idx + 1:] if intro_idx < len(lines) else []

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
            organizer_site_url_raw=organizer_site_url_raw,
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

            resolved_url, resolved_domain, official_url, official_domain, status, query = await self.resolve_organizer_urls(
                context=context,
                title=row.title or summary["title"],
                organizer_site_url_raw=row.organizer_site_url_raw,
                plan_source=row.plan_source,
                applicable_region=row.applicable_region,
            )
            row.organizer_search_query = query
            row.organizer_site_url = resolved_url
            row.organizer_site_domain = resolved_domain
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
                await asyncio.sleep(max(crawl_delay, 0.3))
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
        "organizer_site_url_raw",
        "organizer_search_query",
        "organizer_site_url",
        "organizer_site_domain",
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
            "organizer_site_url_raw",
            "organizer_search_query",
            "organizer_site_url",
            "organizer_site_domain",
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
            if header in {"title", "detail_url", "organizer_site_url_raw", "organizer_site_url", "official_organizer_site_url", "background", "writing_tips", "raw_text"}:
                ws.column_dimensions[letter].width = 42 if "url" in header else 36
            elif header in {"eligible_targets", "deadline_text", "plan_source", "organizer_search_query"}:
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
