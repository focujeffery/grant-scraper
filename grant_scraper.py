
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://dayseechat.com"
LISTING_URL = f"{BASE_URL}/explore-grant/"
OUTPUT_FILE = "daysee_grants.xlsx"
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
GOOGLE_HOSTS = {"google.com", "www.google.com", "google.com.tw", "www.google.com.tw"}
SEARCH_EXCLUDE = {
    "google.com", "www.google.com", "google.com.tw", "www.google.com.tw",
    "bing.com", "www.bing.com", "duckduckgo.com", "www.duckduckgo.com", "html.duckduckgo.com",
    "facebook.com", "www.facebook.com", "instagram.com", "www.instagram.com",
    "threads.net", "www.threads.net", "youtube.com", "www.youtube.com",
    "dayseechat.com", "www.dayseechat.com",
}
OFFICIAL_DOMAIN_HINTS = (".gov.tw", ".gov", "gov.taipei", "gov.kaohsiung", "gov.taichung", "gov.tainan")
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"

SOURCE_DOMAIN_MAP = {
    "勞動部": ["wlb.mol.gov.tw", "mol.gov.tw", "wda.gov.tw"],
    "經濟部": ["sme.gov.tw", "moea.gov.tw", "industry.gov.tw", "startup.sme.gov.tw"],
    "數位發展部": ["digiplus.adi.gov.tw", "adi.gov.tw", "moda.gov.tw"],
    "文化部": ["grants.moc.gov.tw", "moc.gov.tw"],
    "教育部": ["moe.gov.tw", "edu.tw"],
    "衛生福利部": ["mohw.gov.tw", "hpa.gov.tw"],
    "客家委員會": ["hakka.gov.tw"],
    "原住民族委員會": ["cip.gov.tw"],
    "海洋委員會": ["oac.gov.tw", "oca.gov.tw"],
    "國家發展委員會": ["ndc.gov.tw"],
    "農業部": ["moa.gov.tw", "afa.gov.tw"],
    "環境部": ["moenv.gov.tw", "epa.gov.tw"],
    "縣市政府": ["gov.tw"],
}

SOURCE_PORTAL_MAP = {
    "勞動部": ["https://wlb.mol.gov.tw/page/Grants/GrantApply.aspx", "https://wlb.mol.gov.tw/"],
    "經濟部": ["https://startup.sme.gov.tw/home/modules/funding/","https://www.sme.gov.tw/"],
    "數位發展部": ["https://digiplus.adi.gov.tw/plan_table_n.html", "https://digiplus.adi.gov.tw/"],
    "文化部": ["https://grants.moc.gov.tw/Web/","https://www.moc.gov.tw/"],
    "教育部": ["https://www.edu.tw/", "https://www.moe.gov.tw/"],
    "衛生福利部": ["https://www.mohw.gov.tw/"],
    "客家委員會": ["https://www.hakka.gov.tw/"],
    "原住民族委員會": ["https://www.cip.gov.tw/"],
    "海洋委員會": ["https://www.oac.gov.tw/"],
    "國家發展委員會": ["https://www.ndc.gov.tw/"],
    "農業部": ["https://www.moa.gov.tw/"],
    "環境部": ["https://www.moenv.gov.tw/"],
}

REGION_DOMAIN_MAP = {
    "台北": ["taipei.gov.tw"], "臺北": ["taipei.gov.tw"],
    "新北": ["ntpc.gov.tw"],
    "桃園": ["tycg.gov.tw"],
    "台中": ["taichung.gov.tw"], "臺中": ["taichung.gov.tw"],
    "台南": ["tainan.gov.tw"], "臺南": ["tainan.gov.tw"],
    "高雄": ["kcg.gov.tw"],
    "基隆": ["klcg.gov.tw"],
    "新竹": ["hccg.gov.tw", "hsinchu.gov.tw"],
    "彰化": ["changhua.gov.tw"],
    "雲林": ["yunlin.gov.tw"],
    "南投": ["nantou.gov.tw"],
    "台東": ["taitung.gov.tw"], "臺東": ["taitung.gov.tw"],
    "連江": ["matsu.gov.tw"],
}

REGION_PORTAL_MAP = {
    "台北": ["https://www.taipei.gov.tw/"], "臺北": ["https://www.taipei.gov.tw/"],
    "新北": ["https://www.ntpc.gov.tw/"],
    "桃園": ["https://www.tycg.gov.tw/"],
    "台中": ["https://www.taichung.gov.tw/"], "臺中": ["https://www.taichung.gov.tw/"],
    "台南": ["https://www.tainan.gov.tw/"], "臺南": ["https://www.tainan.gov.tw/"],
    "高雄": ["https://www.kcg.gov.tw/"],
    "基隆": ["https://www.klcg.gov.tw/"],
    "新竹": ["https://www.hccg.gov.tw/", "https://www.hsinchu.gov.tw/"],
    "彰化": ["https://www.chcg.gov.tw/"],
    "雲林": ["https://www.yunlin.gov.tw/"],
    "南投": ["https://www.nantou.gov.tw/"],
    "台東": ["https://www.taitung.gov.tw/"], "臺東": ["https://www.taitung.gov.tw/"],
    "連江": ["https://www.matsu.gov.tw/"],
}

TITLE_PORTAL_HINTS = [
    (re.compile(r"工作與生活平衡|工作生活平衡"), "https://wlb.mol.gov.tw/page/Grants/GrantApply.aspx"),
    (re.compile(r"數位服務創新補助計畫"), "https://digiplus.adi.gov.tw/plan_table_n.html"),
    (re.compile(r"服務創新補助計畫"), "https://digiplus.adi.gov.tw/plan_table_n.html"),
]

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
    return [normalize_text(x) for x in text.splitlines() if normalize_text(x)]

def get_domain(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""

def is_google_search_url(url: str) -> bool:
    p = urlparse(url)
    return p.netloc.lower() in GOOGLE_HOSTS and p.path in {"/search", "/url"}

def extract_query_from_google_url(url: str) -> str:
    p = urlparse(url)
    params = parse_qs(p.query)
    if "q" in params and params["q"]:
        return normalize_text(unquote(params["q"][0]))
    return ""

def split_multi_value_text(value: str) -> List[str]:
    if not value:
        return []
    return [normalize_text(x) for x in re.split(r"[｜|/、,，\s]+", value) if normalize_text(x)]

def is_official_domain(domain: str) -> bool:
    domain = (domain or "").lower().strip()
    if not domain or domain in SEARCH_EXCLUDE:
        return False
    if any(domain.endswith(h) for h in OFFICIAL_DOMAIN_HINTS):
        return True
    official_keywords = ("gov.tw", "gov", "moda", "moc.gov.tw", "moa.gov.tw", "mol.gov.tw", "mohw.gov.tw", "hakka.gov.tw", "cip.gov.tw")
    return any(k in domain for k in official_keywords)

def tokenize(text: str) -> List[str]:
    tokens = re.findall(r"[A-Za-z0-9\u4e00-\u9fff]{2,}", (text or "").lower())
    stop = {"年度", "計畫", "補助", "申請", "推動", "政府", "網站", "主辦", "單位", "官方"}
    return [t for t in tokens if t not in stop]

def collect_hint_domains(plan_source: str, applicable_region: str) -> List[str]:
    domains: List[str] = []
    for key, vals in SOURCE_DOMAIN_MAP.items():
        if key and key in (plan_source or ""):
            domains.extend(vals)
    for region in split_multi_value_text(applicable_region):
        for key, vals in REGION_DOMAIN_MAP.items():
            if key and key in region:
                domains.extend(vals)
    out, seen = [], set()
    for d in domains:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out

def collect_portal_hints(title: str, plan_source: str, applicable_region: str) -> List[str]:
    hints: List[str] = []
    for regex, url in TITLE_PORTAL_HINTS:
        if regex.search(title or ""):
            hints.append(url)
    for key, vals in SOURCE_PORTAL_MAP.items():
        if key and key in (plan_source or ""):
            hints.extend(vals)
    for region in split_multi_value_text(applicable_region):
        for key, vals in REGION_PORTAL_MAP.items():
            if key and key in region:
                hints.extend(vals)
    out, seen = [], set()
    for u in hints:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def score_candidate(url: str, anchor_text: str, title: str, plan_source: str, applicable_region: str, query: str, hint_domains: List[str]) -> int:
    domain = get_domain(url)
    if not url.startswith("http") or not domain or domain in SEARCH_EXCLUDE:
        return -10**9
    lower_url = url.lower()
    score = 0
    if url.startswith("https://"):
        score += 5
    if is_official_domain(domain):
        score += 220
    for hint in hint_domains:
        if hint == "gov.tw":
            if domain.endswith(".gov.tw") or ".gov.tw" in domain:
                score += 120
        elif domain.endswith(hint) or hint in lower_url:
            score += 140
    for tok in tokenize(title)[:8]:
        if tok in lower_url or tok in anchor_text.lower():
            score += 12
    for tok in tokenize(plan_source)[:5]:
        if tok in lower_url or tok in anchor_text.lower():
            score += 18
    for tok in tokenize(applicable_region)[:4]:
        if tok in lower_url or tok in anchor_text.lower():
            score += 14
    for tok in tokenize(query)[:8]:
        if tok in lower_url or tok in anchor_text.lower():
            score += 8
    if any(x in lower_url for x in ("grant", "subsidy", "apply", "application", "plan", "project", "news", "download")):
        score += 6
    if lower_url.endswith(".pdf"):
        score += 4
    if domain.endswith(".org.tw"):
        score += 4
    return score

def fetch_url(url: str, timeout: int = 12) -> str:
    req = Request(url, headers={"User-Agent": USER_AGENT, "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8"})
    with urlopen(req, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")

def ddg_search(query: str) -> List[Tuple[str, str]]:
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        html = fetch_url(url, timeout=15)
    except Exception:
        return []
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for a in soup.select("a.result__a, a[href]"):
        href = normalize_text(a.get("href", ""))
        text = normalize_text(a.get_text(" ", strip=True))
        if not href:
            continue
        if href.startswith("//"):
            href = "https:" + href
        if href.startswith("/l/"):
            p = urlparse(href)
            qs = parse_qs(p.query)
            href = normalize_text((qs.get("uddg") or [""])[0])
        if href.startswith("http"):
            results.append((href, text))
    return results

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
        except Exception:
            return

    async def _new_context(self, playwright) -> BrowserContext:
        browser = await playwright.chromium.launch(headless=True)
        return await browser.new_context(locale="zh-TW", extra_http_headers={"Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8"})

    async def _extract_current_page_cards(self, page: Page) -> List[Dict[str, str]]:
        cards = await page.eval_on_selector_all(
            'a[href*="/subsidy/grant-"]',
            """(anchors) => anchors.map(a => ({href: a.href, title: (a.textContent || '').trim()}))
                  .filter(x => /\\/subsidy\\/grant-\\d+\\/?$/.test(x.href) && x.title.length > 0)""",
        )
        out, seen = [], set()
        for c in cards:
            href = normalize_text(c.get("href"))
            title = normalize_text(c.get("title"))
            if href and title and href not in seen:
                seen.add(href)
                out.append({"href": href, "title": title})
        return out

    async def _expected_total(self, page: Page) -> Optional[int]:
        text = await page.locator("body").inner_text()
        m = re.search(r"(\d+)\s+out\s+of\s+(\d+)\s+results", text, re.I)
        return int(m.group(2)) if m else None

    async def _go_next_page(self, page: Page) -> bool:
        next_btn = page.locator("a.ts-load-next").first
        if await next_btn.count() == 0:
            return False
        classes = (await next_btn.get_attribute("class")) or ""
        if "ts-btn-disabled" in classes:
            return False
        before = {x["href"] for x in await self._extract_current_page_cards(page)}
        try:
            await next_btn.evaluate("el => el.click()")
        except Exception:
            try:
                await next_btn.click(timeout=1200, force=True)
            except Exception:
                return False
        await page.wait_for_timeout(1200)
        for _ in range(10):
            await page.wait_for_timeout(300)
            after = {x["href"] for x in await self._extract_current_page_cards(page)}
            if after and after != before:
                return True
        return False

    async def extract_listing(self, page: Page) -> List[Dict[str, str]]:
        await page.goto(self.listing_url, wait_until="networkidle")
        await self._apply_stealth(page)
        await page.wait_for_selector('a[href*="/subsidy/grant-"]')
        expected = await self._expected_total(page)
        all_cards: Dict[str, Dict[str, str]] = {}
        page_no = 1
        while page_no <= 30:
            logger.info("Scanning listing page %d", page_no)
            for c in await self._extract_current_page_cards(page):
                all_cards.setdefault(c["href"], c)
            if expected and len(all_cards) >= expected:
                break
            if not await self._go_next_page(page):
                break
            page_no += 1
        logger.info("Found %d grants", len(all_cards))
        return list(all_cards.values())

    def _trim_to_main_content(self, lines: List[str]) -> List[str]:
        cut_markers = ["返回主頁", "你可能也會喜歡這些資訊", "無符合結果", "上一頁 下一頁"]
        for idx, line in enumerate(lines):
            if any(marker in line for marker in cut_markers):
                return lines[:idx]
        return lines

    def _canonical_label(self, value: str) -> str:
        return normalize_text(value.replace("：", ":").rstrip(":"))

    def _parse_meta(self, meta_lines: List[str]) -> Dict[str, str]:
        label_map = {"計畫來源": "plan_source", "補助對象": "eligible_targets", "適用地區": "applicable_region", "補助金額": "grant_amount", "截止日期": "deadline_date"}
        parsed = {v: "" for v in label_map.values()}
        parsed["deadline_text"] = ""
        parsed["topics"] = ""
        i = 0
        while i < len(meta_lines):
            label = self._canonical_label(meta_lines[i])
            if label not in label_map:
                i += 1
                continue
            j = i + 1
            values = []
            while j < len(meta_lines):
                nxt = self._canonical_label(meta_lines[j])
                line = meta_lines[j]
                if nxt in label_map or "本資訊為AI生成工具" in line:
                    break
                if line.startswith("申請文件"):
                    j += 1
                    continue
                values.append(line)
                j += 1
            field = label_map[label]
            if field == "deadline_date":
                date_val = ""
                text_vals = []
                for v in values:
                    if DATE_RE.fullmatch(v) and not date_val:
                        date_val = v
                    else:
                        text_vals.append(v)
                parsed["deadline_date"] = date_val
                parsed["deadline_text"] = "｜".join(text_vals)
                parsed["topics"] = "｜".join(text_vals)
            else:
                parsed[field] = "｜".join(values)
            i = j
        return parsed

    def _extract_sections(self, content_lines: List[str]) -> Dict[str, List[str]]:
        sections = {"計畫背景": [], "計畫重點": [], "撰寫技巧": []}
        current = None
        for line in content_lines:
            if line in sections:
                current = line
                continue
            if current:
                sections[current].append(line)
        return sections

    def _best_embedded_link(self, links: List[Tuple[str, str]], title: str, plan_source: str, applicable_region: str, query: str) -> Tuple[str, str, bool]:
        hint_domains = collect_hint_domains(plan_source, applicable_region)
        best = ("", "", -10**9, False)
        for href, anchor_text in links:
            score = score_candidate(href, anchor_text, title, plan_source, applicable_region, query, hint_domains)
            domain = get_domain(href)
            official = is_official_domain(domain) or any(domain.endswith(h) for h in hint_domains if h != "gov.tw")
            if "gov.tw" in hint_domains and domain.endswith(".gov.tw"):
                official = True
            if score > best[2]:
                best = (href, domain, score, official)
        return best[0], best[1], best[3]

    def _best_search_result(self, candidates: List[Tuple[str, str]], title: str, plan_source: str, applicable_region: str, query: str) -> Tuple[str, str, bool]:
        hint_domains = collect_hint_domains(plan_source, applicable_region)
        best = ("", "", -10**9, False)
        for href, text in candidates:
            domain = get_domain(href)
            if not href or domain in SEARCH_EXCLUDE:
                continue
            score = score_candidate(href, text, title, plan_source, applicable_region, query, hint_domains)
            official = is_official_domain(domain) or any(domain.endswith(h) for h in hint_domains if h != "gov.tw")
            if "gov.tw" in hint_domains and domain.endswith(".gov.tw"):
                official = True
            if score > best[2]:
                best = (href, domain, score, official)
        return best[0], best[1], best[3]

    def resolve_organizer_urls(
        self,
        title: str,
        organizer_site_url_raw: str,
        plan_source: str,
        applicable_region: str,
        embedded_links: List[Tuple[str, str]],
    ) -> Tuple[str, str, str, str, str, str]:
        raw = normalize_text(organizer_site_url_raw)
        raw_domain = get_domain(raw)
        if raw and raw.startswith("http") and not is_google_search_url(raw):
            status = "direct_official" if is_official_domain(raw_domain) else "direct_non_google"
            official_url = raw if status == "direct_official" else ""
            official_domain = raw_domain if official_url else ""
            return raw, raw_domain, official_url, official_domain, status, ""

        query = extract_query_from_google_url(raw) or normalize_text(title)
        cache_key = f"{query}|{plan_source}|{applicable_region}"
        if cache_key in self.search_cache:
            resolved_url, resolved_domain, official_url, official_domain, status = self.search_cache[cache_key]
            return resolved_url, resolved_domain, official_url, official_domain, status, query

        embedded_url, embedded_domain, embedded_official = self._best_embedded_link(
            embedded_links, title, plan_source, applicable_region, query
        )
        if embedded_url:
            status = "embedded_official" if embedded_official else "embedded_candidate"
            official_url = embedded_url if embedded_official else ""
            official_domain = embedded_domain if embedded_official else ""
            self.search_cache[cache_key] = (embedded_url, embedded_domain, official_url, official_domain, status)
            return embedded_url, embedded_domain, official_url, official_domain, status, query

        portal_hints = collect_portal_hints(title, plan_source, applicable_region)
        search_candidates: List[Tuple[str, str]] = []
        hint_domains = collect_hint_domains(plan_source, applicable_region)
        queries = []
        if hint_domains:
            for dom in hint_domains[:2]:
                queries.append(f'"{title}" site:{dom}')
        if plan_source:
            queries.append(f'"{title}" "{plan_source}"')
        queries.append(f'"{title}"')
        seen = set()
        queries = [q for q in queries if q and not (q in seen or seen.add(q))]

        for q in queries[:2]:
            try:
                logger.info("Resolving URL via lightweight search: %s", q)
                search_candidates.extend(ddg_search(q)[:8])
            except Exception:
                pass
            best_url, best_domain, best_official = self._best_search_result(search_candidates, title, plan_source, applicable_region, q)
            if best_url:
                status = "search_official_match" if best_official else "search_best_effort"
                official_url = best_url if best_official else ""
                official_domain = best_domain if best_official else ""
                self.search_cache[cache_key] = (best_url, best_domain, official_url, official_domain, status)
                return best_url, best_domain, official_url, official_domain, status, query

        if portal_hints:
            hint_url = portal_hints[0]
            hint_domain = get_domain(hint_url)
            status = "hint_portal_fallback"
            self.search_cache[cache_key] = (hint_url, hint_domain, hint_url, hint_domain, status)
            return hint_url, hint_domain, hint_url, hint_domain, status, query

        if hint_domains:
            if hint_domains[0] == "gov.tw":
                fallback_url = f"https://www.gov.tw/"
                fallback_domain = "www.gov.tw"
            else:
                fallback_domain = hint_domains[0]
                fallback_url = f"https://{fallback_domain}/"
            status = "hint_domain_fallback"
            self.search_cache[cache_key] = (fallback_url, fallback_domain, fallback_url, fallback_domain, status)
            return fallback_url, fallback_domain, fallback_url, fallback_domain, status, query

        if raw:
            status = "search_fallback_original"
            self.search_cache[cache_key] = (raw, raw_domain, "", "", status)
            return raw, raw_domain, "", "", status, query

        return "", "", "", "", "unresolved", query

    def _parse_detail_html(self, url: str, html: str) -> Tuple[GrantRow, List[Tuple[str, str]]]:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        for node in soup.find_all(style=True):
            style_value = (node.get("style") or "").replace(" ", "").lower()
            if "display:none" in style_value:
                node.decompose()

        organizer_site_url_raw = ""
        application_note = ""
        external_links: List[Tuple[str, str]] = []
        for a in soup.find_all("a", href=True):
            txt = normalize_text(a.get_text(" ", strip=True))
            href = normalize_text(urljoin(url, a.get("href", "")))
            domain = get_domain(href)
            if not href.startswith("http") or domain in SEARCH_EXCLUDE:
                continue
            if "申請文件" in txt or "主辦單位網站" in txt:
                organizer_site_url_raw = href
                application_note = txt
            external_links.append((href, txt))

        lines = split_lines(soup.get_text("\n"))
        lines = self._trim_to_main_content(lines)
        title = normalize_text(lines[0] if lines else "")
        title = re.sub(r"\s*-\s*小社區大事件\s*$", "", title)

        disclaimer_idx = next((i for i, x in enumerate(lines) if "本資訊為AI生成工具" in x), len(lines))
        intro_idx = next((i for i, x in enumerate(lines) if "計畫簡介" in x), disclaimer_idx)
        meta_lines = lines[1:disclaimer_idx]
        content_lines = lines[intro_idx + 1:] if intro_idx < len(lines) else []

        parsed = self._parse_meta(meta_lines)
        topics = list(dict.fromkeys([normalize_text(x) for x in parsed.get("topics", "").split("｜") if normalize_text(x)]))[:5]
        sections = self._extract_sections(content_lines)
        key_points = [x.lstrip("•・- ").strip() for x in sections["計畫重點"] if x.lstrip("•・- ").strip()][:5]

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
            background="\n".join(sections["計畫背景"]).strip(),
            writing_tips="\n".join(sections["撰寫技巧"]).strip(),
            raw_text="\n".join(lines).strip(),
        )
        for idx, topic in enumerate(topics, start=1):
            setattr(row, f"topic_{idx}", topic)
        for idx, point in enumerate(key_points, start=1):
            setattr(row, f"key_point_{idx}", point)
        return row, external_links

    async def extract_detail(self, context: BrowserContext, summary: Dict[str, str]) -> GrantRow:
        page = await context.new_page()
        try:
            logger.info("Fetching detail: %s", summary["href"])
            await page.goto(summary["href"], wait_until="networkidle")
            await self._apply_stealth(page)
            await page.wait_for_selector("h1, h2, h3")
            html = await page.content()
            row, external_links = self._parse_detail_html(summary["href"], html)
            if not row.title:
                row.title = re.sub(r"\s*-\s*小社區大事件\s*$", "", summary["title"])

            resolved_url, resolved_domain, official_url, official_domain, status, query = self.resolve_organizer_urls(
                title=row.title or summary["title"],
                organizer_site_url_raw=row.organizer_site_url_raw,
                plan_source=row.plan_source,
                applicable_region=row.applicable_region,
                embedded_links=external_links,
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
        async with async_playwright() as playwright:
            listing_context = await self._new_context(playwright)
            listing_page = await listing_context.new_page()
            listings = await self.extract_listing(listing_page)
            await listing_page.close()
            await listing_context.close()

            rows: List[GrantRow] = []
            detail_context = await self._new_context(playwright)
            for idx, summary in enumerate(listings, start=1):
                row = await self.extract_detail(detail_context, summary)
                rows.append(row)
                await asyncio.sleep(0.15)
                if idx % 10 == 0:
                    logger.info("Processed %d/%d detail pages", idx, len(listings))
            await detail_context.close()
            return rows

def save_to_excel(rows: List[GrantRow], output_path: str = OUTPUT_FILE) -> None:
    data = [asdict(r) for r in rows]
    df = pd.DataFrame(data)
    ordered_cols = [
        "title","detail_url","plan_source","eligible_targets","applicable_region","grant_amount",
        "organizer_site_url_raw","organizer_search_query","organizer_site_url","organizer_site_domain",
        "official_organizer_site_url","official_organizer_domain","official_url_status","application_note",
        "deadline_date","deadline_text","topic_1","topic_2","topic_3","topic_4","topic_5",
        "background","key_point_1","key_point_2","key_point_3","key_point_4","key_point_5","writing_tips","raw_text"
    ]
    for c in ordered_cols:
        if c not in df.columns:
            df[c] = ""
    df = df[ordered_cols]

    summary_cols = [
        "title","plan_source","eligible_targets","applicable_region","grant_amount",
        "deadline_date","deadline_text","topic_1","topic_2","topic_3","topic_4","topic_5",
        "organizer_site_url_raw","organizer_search_query","organizer_site_url","organizer_site_domain",
        "official_organizer_site_url","official_organizer_domain","official_url_status","detail_url"
    ]
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df[summary_cols].to_excel(writer, sheet_name="grants_summary", index=False)
        df.to_excel(writer, sheet_name="grants_detail", index=False)

    wb = load_workbook(output_path)
    for ws in wb.worksheets:
        ws.freeze_panes = "A2"
        for cell in ws[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        wrap_cols = {"A","B","C","D","G","H","I","K","N","P","V","W","X","Y","Z","AA","AB","AC"}
        for col_cells in ws.columns:
            letter = col_cells[0].column_letter
            max_len = max((len(str(c.value)) if c.value is not None else 0) for c in col_cells)
            width = min(max(max_len + 2, 10), 42)
            if letter in {"V","AB","AC"}:
                width = 42
            ws.column_dimensions[letter].width = width
            if letter in wrap_cols:
                for c in col_cells:
                    c.alignment = Alignment(vertical="top", wrap_text=True)
    wb.save(output_path)
    logger.info("Exported %d records to %s", len(df), output_path)

async def main() -> None:
    crawler = GrantCrawler()
    rows = await crawler.run()
    save_to_excel(rows, OUTPUT_FILE)

if __name__ == "__main__":
    asyncio.run(main())
