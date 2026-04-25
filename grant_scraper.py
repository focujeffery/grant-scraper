import asyncio
import logging
import re
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse
from urllib.request import Request, urlopen

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
SEARCH_HOSTS = GOOGLE_HOSTS | {"bing.com", "www.bing.com", "duckduckgo.com", "www.duckduckgo.com", "html.duckduckgo.com"}
SEARCH_EXCLUDE = SEARCH_HOSTS | {
    "facebook.com", "www.facebook.com",
    "instagram.com", "www.instagram.com",
    "threads.net", "www.threads.net",
    "youtube.com", "www.youtube.com",
    "dayseechat.com", "www.dayseechat.com",
    "support.google.com", "www.104.com.tw", "104.com.tw",
}
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

OFFICIAL_DOMAINS_BY_SOURCE = {
    "勞動部": ["mol.gov.tw", "wda.gov.tw", "wlb.mol.gov.tw"],
    "數位發展部": ["moda.gov.tw", "adi.gov.tw", "digiplus.adi.gov.tw"],
    "經濟部": ["moea.gov.tw", "sme.gov.tw", "sbir.org.tw"],
    "文化部": ["moc.gov.tw"],
    "客家委員會": ["hakka.gov.tw"],
    "原住民族委員會": ["cip.gov.tw"],
    "農業部": ["moa.gov.tw", "www.moa.gov.tw"],
    "教育部": ["moe.gov.tw"],
    "衛生福利部": ["mohw.gov.tw"],
    "環境部": ["moenv.gov.tw"],
    "國家科學及技術委員會": ["nstc.gov.tw"],
}

REGION_TO_GOV_DOMAIN = {
    "臺北市": ["gov.taipei", "taipei.gov.tw"],
    "台北市": ["gov.taipei", "taipei.gov.tw"],
    "新北市": ["newtaipei.gov.tw"],
    "桃園市": ["taoyuan.gov.tw"],
    "臺中市": ["taichung.gov.tw"],
    "台中市": ["taichung.gov.tw"],
    "臺南市": ["tainan.gov.tw"],
    "台南市": ["tainan.gov.tw"],
    "高雄市": ["kcg.gov.tw", "kaohsiung.gov.tw"],
    "基隆市": ["klcg.gov.tw"],
    "新竹市": ["hccg.gov.tw"],
    "新竹縣": ["hsinchu.gov.tw"],
    "苗栗縣": ["miaoli.gov.tw"],
    "彰化縣": ["changhua.gov.tw"],
    "南投縣": ["nantou.gov.tw"],
    "雲林縣": ["yunlin.gov.tw"],
    "嘉義市": ["chiayi.gov.tw"],
    "嘉義縣": ["cyhg.gov.tw"],
    "屏東縣": ["ptcg.gov.tw"],
    "宜蘭縣": ["e-land.gov.tw"],
    "花蓮縣": ["hl.gov.tw"],
    "臺東縣": ["taitung.gov.tw"],
    "台東縣": ["taitung.gov.tw"],
    "澎湖縣": ["penghu.gov.tw"],
    "金門縣": ["kinmen.gov.tw"],
    "連江縣": ["matsu.gov.tw"],
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
    if params.get("q"):
        return normalize_text(unquote(params["q"][0]))
    return ""


def is_official_domain(domain: str) -> bool:
    domain = (domain or "").lower().strip()
    if not domain:
        return False
    if domain in SEARCH_EXCLUDE:
        return False
    if domain.endswith(".gov.tw") or domain == "gov.tw" or domain.endswith(".gov"):
        return True
    return any(token in domain for token in [
        "gov.taipei", "gov.kaohsiung", "gov.taichung", "gov.tainan", "gov", "mol.gov.tw",
        "moda.gov.tw", "adi.gov.tw", "digiplus.adi.gov.tw", "hakka.gov.tw", "cip.gov.tw",
        "moc.gov.tw", "moa.gov.tw", "moe.gov.tw", "mohw.gov.tw", "moea.gov.tw", "nstc.gov.tw",
    ])


def tokenize(text: str) -> List[str]:
    tokens = re.findall(r"[A-Za-z0-9\u4e00-\u9fff]{2,}", (text or "").lower())
    stop = {"年度", "計畫", "補助", "申請", "推動", "政府", "網站", "主辦", "單位", "官方", "方案"}
    return [t for t in tokens if t not in stop]


def first_non_empty(values: List[str]) -> str:
    for v in values:
        v = normalize_text(v)
        if v:
            return v
    return ""


def extract_region_tokens(*texts: str) -> List[str]:
    matched: List[str] = []
    for text in texts:
        text = normalize_text(text)
        for key in REGION_TO_GOV_DOMAIN:
            if key and key in text and key not in matched:
                matched.append(key)
    # Also accept short names such as 台中、台北、新北
    short_map = {
        "台北": "台北市", "臺北": "臺北市",
        "新北": "新北市",
        "桃園": "桃園市",
        "台中": "台中市", "臺中": "臺中市",
        "台南": "台南市", "臺南": "臺南市",
        "高雄": "高雄市",
        "基隆": "基隆市",
        "新竹": "新竹市",
        "苗栗": "苗栗縣",
        "彰化": "彰化縣",
        "南投": "南投縣",
        "雲林": "雲林縣",
        "嘉義": "嘉義市",
        "屏東": "屏東縣",
        "宜蘭": "宜蘭縣",
        "花蓮": "花蓮縣",
        "台東": "台東縣", "臺東": "臺東縣",
        "澎湖": "澎湖縣",
        "金門": "金門縣",
        "連江": "連江縣",
    }
    for text in texts:
        text = normalize_text(text)
        for short, full in short_map.items():
            if short in text and full not in matched:
                matched.append(full)
    return matched


def source_domain_hints(plan_source: str, applicable_region: str, title: str = "") -> List[str]:
    hints: List[str] = []
    for key, vals in OFFICIAL_DOMAINS_BY_SOURCE.items():
        if key and key in (plan_source or ""):
            hints.extend(vals)
    for region in extract_region_tokens(applicable_region, title):
        hints.extend(REGION_TO_GOV_DOMAIN.get(region, []))
    deduped: List[str] = []
    for h in hints:
        if h not in deduped:
            deduped.append(h)
    return deduped


def clean_title_for_search(title: str) -> str:
    title = normalize_text(title)
    title = re.sub(r"^(?:中華民國)?\d{2,4}年度", "", title)
    title = re.sub(r"^(?:中華民國)?\d{2,4}年", "", title)
    title = re.sub(r"^第\d+期", "", title)
    title = re.sub(r"[【】\[\]()（）]", " ", title)
    title = normalize_text(title)
    return title


def build_local_bing_queries(title: str, applicable_region: str, domains: List[str]) -> List[str]:
    full_title = normalize_text(title)
    core_title = clean_title_for_search(title)
    regions = extract_region_tokens(applicable_region, title)
    region_terms = [normalize_text(r.replace("市", "").replace("縣", "")) for r in regions]
    generic_trim = re.sub(r"(補助計畫|實施計畫|獎勵計畫|計畫|補助|方案)$", "", core_title).strip()
    base_terms = [full_title, core_title, generic_trim]
    base_terms = [t for t in base_terms if t]
    queries: List[str] = []
    for domain in domains:
        for term in base_terms:
            queries.append(f'"{term}" site:{domain}')
            queries.append(f'{term} site:{domain}')
            queries.append(f'{term} 公告 site:{domain}')
            queries.append(f'{term} 補助 site:{domain}')
            queries.append(f'{term} filetype:pdf site:{domain}')
            for region in region_terms[:2]:
                if region and region not in term:
                    queries.append(f'{region} {term} site:{domain}')
    # Deduplicate while preserving order
    seen = set()
    return [q for q in queries if q and not (q in seen or seen.add(q))]


def score_candidate(url: str, text: str, title: str, plan_source: str, applicable_region: str, query: str) -> int:
    domain = get_domain(url)
    lower_url = url.lower()
    lower_text = (text or "").lower()
    if not url.startswith("http"):
        return -10**9
    if domain in SEARCH_EXCLUDE:
        return -10**9

    score = 0
    if url.startswith("https://"):
        score += 5
    if is_official_domain(domain):
        score += 220
    if domain.endswith(".org.tw"):
        score += 5
    if lower_url.endswith(".pdf"):
        score += 12

    hints = source_domain_hints(plan_source, applicable_region, title)
    for hint in hints:
        if hint in domain or hint in lower_url:
            score += 120

    path = urlparse(url).path or "/"
    if path in {"", "/"}:
        score -= 35
    if any(seg in lower_url for seg in ["news", "content", "article", "cp.aspx", "download", "file", "bulletin", "apply"]):
        score += 10

    cleaned_title = clean_title_for_search(title)
    for token in tokenize(cleaned_title)[:12]:
        if token in lower_url:
            score += 18
        if token in lower_text:
            score += 12
    for token in tokenize(plan_source)[:5]:
        if token in lower_url or token in lower_text:
            score += 20
    for token in tokenize(applicable_region)[:5]:
        if token in lower_url or token in lower_text:
            score += 16
    for token in tokenize(query)[:10]:
        if token in lower_url or token in lower_text:
            score += 5

    # Penalize irrelevant common portals.
    if any(bad in domain for bad in ["104.com.tw", "104", "youtube.com", "facebook.com", "instagram.com"]):
        score -= 500
    return score


def domain_variants(domain: str) -> List[str]:
    domain = normalize_text((domain or "").lower())
    if not domain:
        return []
    variants = []
    for candidate in [domain, domain.removeprefix("www."), f"www.{domain.removeprefix('www.')}"]:
        candidate = candidate.strip(".")
        if candidate and candidate not in variants:
            variants.append(candidate)
    return variants


def domain_matches_hint(domain: str, hints: List[str]) -> bool:
    domain = (domain or "").lower()
    if not domain:
        return False
    for hint in hints:
        for variant in domain_variants(hint):
            if domain == variant or domain.endswith("." + variant):
                return True
    return False


def canonicalize_url(url: str) -> str:
    url = normalize_text(url)
    if not url:
        return ""
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + url.lstrip("/")
    return url


def verify_url(url: str, allowed_domains: Optional[List[str]] = None) -> Tuple[str, str]:
    url = canonicalize_url(url)
    if not url:
        return "", ""
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    candidate_urls: List[str] = []

    if host:
        host_candidates = domain_variants(host)
        for candidate_host in host_candidates:
            rebuilt = parsed._replace(netloc=candidate_host)
            candidate_urls.append(rebuilt.geturl())
            if not rebuilt.path:
                candidate_urls.append(rebuilt._replace(path="/").geturl())
    else:
        candidate_urls.append(url)

    seen = set()
    candidate_urls = [u for u in candidate_urls if not (u in seen or seen.add(u))]

    for candidate in candidate_urls:
        req = Request(
            candidate,
            headers={
                "User-Agent": USER_AGENT,
                "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
            },
        )
        try:
            with urlopen(req, timeout=20) as resp:
                final_url = normalize_text(resp.geturl() or candidate)
                final_domain = get_domain(final_url)
                if final_domain in SEARCH_EXCLUDE:
                    continue
                if allowed_domains and not domain_matches_hint(final_domain, allowed_domains):
                    continue
                return final_url, final_domain
        except Exception:
            continue
    return "", ""


def choose_best_candidates(
    candidates: List[Tuple[str, str]],
    title: str,
    plan_source: str,
    applicable_region: str,
    query: str,
    allowed_domains: Optional[List[str]] = None,
) -> Tuple[str, str, str, str, str]:
    deduped: Dict[str, Tuple[str, str]] = {}
    for href, text in candidates:
        href = canonicalize_url(href)
        if not href:
            continue
        domain = get_domain(href)
        if not domain or domain in SEARCH_EXCLUDE:
            continue
        if allowed_domains and not domain_matches_hint(domain, allowed_domains):
            continue
        deduped[href] = (href, normalize_text(text))

    best_any = ("", "", -10**9)
    best_official = ("", "", -10**9)
    for href, text in deduped.values():
        verified_url, verified_domain = verify_url(href, allowed_domains=allowed_domains)
        if not verified_url:
            continue
        score = score_candidate(verified_url, text, title, plan_source, applicable_region, query)
        if score > best_any[2]:
            best_any = (verified_url, verified_domain, score)
        if is_official_domain(verified_domain) and score > best_official[2]:
            best_official = (verified_url, verified_domain, score)

    organizer_url = best_official[0] or best_any[0]
    organizer_domain = best_official[1] or best_any[1]
    official_url = best_official[0]
    official_domain = best_official[1]
    if official_url:
        status = "bing_title_official"
    elif organizer_url:
        status = "bing_title_best_effort"
    else:
        status = "bing_title_no_match"
    return organizer_url, organizer_domain, official_url, official_domain, status


def bing_search(query: str, max_results: int = 10) -> List[Tuple[str, str]]:
    url = f"https://www.bing.com/search?format=rss&q={quote_plus(query)}&setlang=zh-Hant&cc=tw"
    req = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        },
    )
    with urlopen(req, timeout=20) as resp:
        xml = resp.read().decode("utf-8", errors="ignore")
    soup = BeautifulSoup(xml, "xml")
    results: List[Tuple[str, str]] = []
    for item in soup.find_all("item"):
        href = normalize_text(item.link.get_text(" ", strip=True) if item.link else "")
        title_text = normalize_text(item.title.get_text(" ", strip=True) if item.title else "")
        desc = normalize_text(item.description.get_text(" ", strip=True) if item.description else "")
        text = normalize_text(f"{title_text} {desc}")
        if href.startswith("http"):
            results.append((href, text))
        if len(results) >= max_results:
            break
    return results


def verified_homepage_from_hints(hints: List[str]) -> Tuple[str, str]:
    for hint in hints:
        for variant in domain_variants(hint):
            verified_url, verified_domain = verify_url(f"https://{variant}/", allowed_domains=[hint])
            if verified_url and verified_domain:
                return verified_url, verified_domain
    return "", ""


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
              .map((a) => ({ href: a.href, title: (a.textContent || '').trim() }))
              .filter((x) => /\/subsidy\/grant-\d+\/?$/.test(x.href) && x.title.length > 0)
            """,
        )
        deduped: Dict[str, Dict[str, str]] = {}
        for card in cards:
            href = card["href"].strip()
            title = normalize_text(card.get("title") or "")
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
        while page_index <= 30:
            logger.info("Scanning listing page %d", page_index)
            for card in await self._extract_current_page_cards(page):
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
        return normalize_text(clean.rstrip(":"))

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
                if next_label in label_map or current_line.startswith("申請文件") or "本資訊為AI生成工具" in current_line:
                    break
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
        key_points = []
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
        for idx, topic in enumerate(topic_candidates, start=1):
            setattr(row, f"topic_{idx}", topic)
        for idx, point in enumerate(key_points, start=1):
            setattr(row, f"key_point_{idx}", point)
        return row

    def _resolve_via_bing_title(self, title: str, plan_source: str, applicable_region: str) -> Tuple[str, str, str, str, str, str]:
        title = normalize_text(title)
        plan_source = normalize_text(plan_source)
        applicable_region = normalize_text(applicable_region)
        if not title:
            return "", "", "", "", "no_title", ""

        cache_key = f"{title}|{plan_source}|{applicable_region}"
        if cache_key in self.search_cache:
            organizer_url, organizer_domain, official_url, official_domain, status = self.search_cache[cache_key]
            return organizer_url, organizer_domain, official_url, official_domain, status, title

        source_hints = source_domain_hints(plan_source, applicable_region, title)
        regions = extract_region_tokens(applicable_region, title)
        is_local_gov = ("縣市政府" in plan_source) or bool(regions)

        queries: List[str] = []
        if is_local_gov and source_hints:
            queries.extend(build_local_bing_queries(title, applicable_region, source_hints[:4]))

        cleaned_title = clean_title_for_search(title)
        generic_queries = []
        for hint in source_hints[:3]:
            for variant in domain_variants(hint)[:2]:
                generic_queries.append(f'"{title}" site:{variant}')
                generic_queries.append(f'"{cleaned_title}" site:{variant}')
                generic_queries.append(f'{cleaned_title} 公告 site:{variant}')
                generic_queries.append(f'{cleaned_title} 補助 site:{variant}')
                generic_queries.append(f'{cleaned_title} filetype:pdf site:{variant}')
        generic_queries.extend([
            f'{title} {plan_source} site:gov.tw',
            f'{cleaned_title} {plan_source} site:gov.tw',
            f'{title} {applicable_region} site:gov.tw',
            f'{cleaned_title} {applicable_region} site:gov.tw',
            f'{title} {plan_source}',
            f'{cleaned_title} {plan_source}',
            title,
            cleaned_title,
        ])

        seen_all = set()
        queries.extend([q for q in generic_queries if q and not (q in seen_all or seen_all.add(q))])

        all_candidates: List[Tuple[str, str]] = []
        used_query = queries[0] if queries else title

        for idx, q in enumerate(queries):
            used_query = q
            logger.info("Resolving organizer URL via Bing title search: %s", q)
            try:
                candidates = bing_search(q)
            except Exception as exc:
                logger.warning("Bing lookup failed for %s: %s", q, exc)
                continue
            all_candidates.extend(candidates)
            organizer_url, organizer_domain, official_url, official_domain, status = choose_best_candidates(
                all_candidates,
                title,
                plan_source,
                applicable_region,
                q,
                allowed_domains=source_hints or None,
            )
            if official_url:
                status = "bing_region_official_verified" if is_local_gov else "bing_title_official_verified"
                self.search_cache[cache_key] = (organizer_url, organizer_domain, official_url, official_domain, status)
                return organizer_url, organizer_domain, official_url, official_domain, status, used_query
            if organizer_url and idx >= 2:
                # only stop early after we have a verified non-empty candidate
                status = "bing_region_best_effort_verified" if is_local_gov else "bing_title_best_effort_verified"
                self.search_cache[cache_key] = (organizer_url, organizer_domain, official_url, official_domain, status)
                return organizer_url, organizer_domain, official_url, official_domain, status, used_query

        organizer_url, organizer_domain, official_url, official_domain, status = choose_best_candidates(
            all_candidates,
            title,
            plan_source,
            applicable_region,
            used_query or title,
            allowed_domains=source_hints or None,
        )

        if official_url:
            status = "bing_region_official_verified" if is_local_gov else "bing_title_official_verified"
        elif organizer_url:
            status = "bing_region_best_effort_verified" if is_local_gov else "bing_title_best_effort_verified"
        else:
            verified_home, verified_domain = verified_homepage_from_hints(source_hints)
            if verified_home and verified_domain:
                organizer_url = verified_home
                organizer_domain = verified_domain
                official_url = verified_home
                official_domain = verified_domain
                status = "verified_portal_homepage"
            else:
                status = "bing_title_no_match"

        self.search_cache[cache_key] = (organizer_url, organizer_domain, official_url, official_domain, status)
        return organizer_url, organizer_domain, official_url, official_domain, status, used_query or title

    async def resolve_organizer_urls(self, row: GrantRow) -> GrantRow:
        raw = normalize_text(row.organizer_site_url_raw)
        raw_domain = get_domain(raw)
        if raw.startswith("http") and not is_google_search_url(raw) and raw_domain not in SEARCH_EXCLUDE:
            row.organizer_search_query = row.title
            verified_raw, verified_domain = verify_url(raw)
            row.organizer_site_url = verified_raw or raw
            row.organizer_site_domain = verified_domain or raw_domain
            if is_official_domain(row.organizer_site_domain):
                row.official_organizer_site_url = row.organizer_site_url
                row.official_organizer_domain = row.organizer_site_domain
                row.official_url_status = "direct_official"
            else:
                row.official_url_status = "direct_non_google"
            return row

        organizer_url, organizer_domain, official_url, official_domain, status, used_query = await asyncio.to_thread(
            self._resolve_via_bing_title,
            row.title,
            row.plan_source,
            row.applicable_region,
        )
        row.organizer_search_query = used_query
        row.organizer_site_url = organizer_url or raw
        row.organizer_site_domain = organizer_domain or get_domain(row.organizer_site_url)
        row.official_organizer_site_url = official_url
        row.official_organizer_domain = official_domain
        row.official_url_status = status
        return row

    async def extract_detail(self, page: Page, summary: Dict[str, str]) -> GrantRow:
        logger.info("Fetching detail: %s", summary["href"])
        await page.goto(summary["href"], wait_until="networkidle")
        await self._apply_stealth(page)
        await page.wait_for_selector("h1, h2, h3")
        html = await page.content()
        row = self._parse_detail_html(summary["href"], html)
        if not row.title:
            row.title = re.sub(r"\s*-\s*小社區大事件\s*$", "", summary["title"])
        row = await self.resolve_organizer_urls(row)
        return row

    async def run(self) -> List[GrantRow]:
        crawl_delay = await get_crawl_delay(self.base_url)
        async with async_playwright() as playwright:
            context = await self._new_context(playwright)
            page = await context.new_page()
            listings = await self.extract_listing(page)
            output: List[GrantRow] = []
            for idx, summary in enumerate(listings, start=1):
                row = await self.extract_detail(page, summary)
                output.append(row)
                await asyncio.sleep(max(crawl_delay, 0.2))
                if idx % 10 == 0:
                    logger.info("Processed %d/%d detail pages", idx, len(listings))
            await page.close()
            await context.close()
            return output


def save_to_excel(rows: List[GrantRow], output_path: str = OUTPUT_FILE) -> None:
    structured_rows = [asdict(r) for r in rows]
    df = pd.DataFrame(structured_rows)
    ordered_cols = [
        "title", "detail_url", "plan_source", "eligible_targets", "applicable_region", "grant_amount",
        "organizer_site_url_raw", "organizer_search_query", "organizer_site_url", "organizer_site_domain",
        "official_organizer_site_url", "official_organizer_domain", "official_url_status", "application_note",
        "deadline_date", "deadline_text", "topic_1", "topic_2", "topic_3", "topic_4", "topic_5",
        "background", "key_point_1", "key_point_2", "key_point_3", "key_point_4", "key_point_5",
        "writing_tips", "raw_text",
    ]
    df = df.reindex(columns=ordered_cols)
    summary_df = df[[
        "title", "plan_source", "eligible_targets", "applicable_region", "grant_amount",
        "deadline_date", "deadline_text", "topic_1", "topic_2", "topic_3", "topic_4", "topic_5",
        "organizer_site_url_raw", "organizer_search_query", "organizer_site_url", "organizer_site_domain",
        "official_organizer_site_url", "official_organizer_domain", "official_url_status", "detail_url",
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
        for col_cells in ws.columns:
            letter = col_cells[0].column_letter
            header = str(col_cells[0].value or "")
            if header in {"title", "detail_url", "organizer_site_url_raw", "organizer_site_url", "official_organizer_site_url", "background", "writing_tips", "raw_text"}:
                ws.column_dimensions[letter].width = 42 if "url" in header else 36
            elif header in {"eligible_targets", "deadline_text", "plan_source", "organizer_search_query"}:
                ws.column_dimensions[letter].width = 26
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
