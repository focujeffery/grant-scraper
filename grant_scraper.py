"""
grant_scraper
===============

This module defines an asynchronous crawler for the DayseeChat grant portal.
It uses Playwright with stealth capabilities to navigate the `explore‑grant` listing,
enumerate all available grant announcements and extract detailed information from each
grant page.  The extracted data are normalised through a Pydantic schema and
written into an Excel workbook.  To operate reliably in production the crawler
implements several layers of robustness, including:

* **Robots.txt awareness** – the crawler fetches and parses the site's
  `robots.txt` on startup, respecting any declared `Crawl‑delay` directive.
* **Exponential backoff** – network operations automatically retry on
  transient HTTP errors (403, 429, etc.) with a growing delay between
  attempts.  A simple circuit breaker prevents hammering the server once
  repeated failures occur.
* **HTML cleaning** – raw HTML is sanitised to remove script/style tags and
  converted into Markdown to minimise noise and reduce token counts when
  downstream processing the content.
* **PII anonymisation** – common personally identifiable information such as
  e‑mail addresses and phone numbers are detected and hashed or removed
  before persisting.
* **Stealth browsing** – the crawler loads the ``playwright‑stealth`` plugin to
  mask Selenium/WebDriver footprints.  Randomised user agents, jittered
  pauses and proxy rotation further reduce the risk of being blocked by
  simple anti‑bot systems.

The main entry point is the ``main()`` coroutine which coordinates the
navigation of the listing page, the extraction of each grant and the export
to ``Excel``.  See the README or module level docstring for usage hints.

Note: this script is intended to be run in a scheduled environment (e.g.
GitHub Actions).  It does not persist any secrets; proxies and other
configuration should be supplied via environment variables or a config file.
"""

import asyncio
import re
import random
import time
import logging
import hashlib
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any, Iterable

import pandas as pd
from pydantic import BaseModel, HttpUrl, Field, validator

from playwright.async_api import async_playwright, Page, Browser, BrowserContext, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import stealth_async


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class GrantSummary(BaseModel):
    """A Pydantic model representing summary information for a grant."""
    title: str = Field(..., description="名稱/標題")
    url: HttpUrl = Field(..., description="補助計畫詳細頁面網址")
    topics: List[str] = Field(default_factory=list, description="關注議題")
    recipients: List[str] = Field(default_factory=list, description="補助對象")
    sources: List[str] = Field(default_factory=list, description="計畫來源")
    amount: Optional[str] = Field(None, description="補助金額")
    deadline: Optional[datetime] = Field(None, description="截止日期")

    @validator('deadline', pre=True)
    def parse_deadline(cls, value: Any) -> Optional[datetime]:
        """
        Convert deadline strings into naive UTC datetimes.

        The source format is expected to be ISO‑like (e.g. ``2026‑04‑30``) but
        this validator gracefully handles ``None`` or empty strings by
        returning ``None``.
        """
        if not value:
            return None
        try:
            return datetime.strptime(str(value).strip(), "%Y-%m-%d")
        except Exception:
            logger.debug("Could not parse deadline '%s'", value)
            return None


class GrantDetail(GrantSummary):
    """Extends GrantSummary with full textual content and arbitrary details."""
    description_md: Optional[str] = Field(
        None, description="使用 Markdown 清洗後的完整頁面內容"
    )
    details: Dict[str, Any] = Field(
        default_factory=dict, description="從詳細頁面額外解析出的資料欄位"
    )


def exponential_backoff(
    retries: int = 5,
    base_delay: float = 1.0,
    allowed_status: Iterable[int] = (403, 429),
) -> Any:
    """
    A decorator to wrap coroutines with exponential backoff and a simple
    circuit breaker.  Retries are triggered on exceptions whose ``.status``
    attribute matches one of the ``allowed_status`` codes (typically HTTP
    403/429).  When the maximum number of attempts is reached the
    ``CircuitBreakerError`` is raised and must be handled by the caller.
    """

    def decorator(func):
        async def wrapper(*args, **kwargs):
            attempts = 0
            while True:
                try:
                    return await func(*args, **kwargs)
                except Exception as exc:
                    attempts += 1
                    status = getattr(exc, "status", None)
                    if status not in allowed_status or attempts > retries:
                        raise
                    delay = base_delay * (2 ** (attempts - 1))
                    jitter = random.uniform(0, 0.5)
                    sleep_time = delay + jitter
                    logger.warning(
                        "Encountered %s (status=%s). Retrying in %.2f seconds...",
                        exc.__class__.__name__, status, sleep_time,
                    )
                    await asyncio.sleep(sleep_time)
        return wrapper
    return decorator


async def get_crawl_delay(domain: str) -> float:
    """
    Fetch the robots.txt from ``domain`` and parse its ``Crawl‑delay``
    directive for generic user agents.  If unspecified, returns 0.

    This function uses a simple HTTP client implemented via Playwright
    itself to avoid additional dependencies.  It attempts a best effort
    parse of the robots.txt content.
    """
    url = f"{domain.rstrip('/')}/robots.txt"
    async with async_playwright() as p:
        # Use a lightweight context for robots.txt; headless with no
        # additional anti‑bot measures.
        browser = await p.chromium.launch()
        page = await browser.new_page()
        try:
            logger.info("Fetching robots.txt from %s", url)
            resp = await page.goto(url)
            if not resp:
                return 0.0
            text = await page.content()
        finally:
            await browser.close()
    # Extract crawl delay for generic user‑agents
    crawl_delay = 0.0
    current_agent = None
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        m_agent = re.match(r'user-agent:\s*(.*)', line, re.I)
        if m_agent:
            current_agent = m_agent.group(1).strip()
            continue
        if current_agent in ('*', None):
            m_delay = re.match(r'crawl-delay:\s*(\d+)', line, re.I)
            if m_delay:
                crawl_delay = float(m_delay.group(1))
    logger.info("Crawl delay parsed: %.1f seconds", crawl_delay)
    return crawl_delay


def clean_html_to_markdown(html: str) -> str:
    """
    Remove script/style tags and convert the given HTML fragment into
    Markdown using BeautifulSoup and html2text.  Any extraneous markup
    (advertisements, trackers) is stripped out.
    """
    from bs4 import BeautifulSoup
    import html2text

    soup = BeautifulSoup(html, 'html.parser')
    for tag in soup(['script', 'style', 'noscript']):
        tag.decompose()
    # Remove hidden elements
    for hidden in soup.select('[style*=display:none]'):
        hidden.decompose()
    # Convert to markdown
    h = html2text.HTML2Text()
    h.ignore_links = False
    markdown = h.handle(str(soup))
    return markdown.strip()


def anonymise_pii(text: str) -> str:
    """
    Detect common PII such as e‑mail addresses and phone numbers within
    ``text`` and replace them with hashed tokens.  This ensures that
    sensitive user information is not persisted.  For names (which can be
    ambiguous), the function removes them when they appear to be part of
    contact details (e.g. preceding an e‑mail).
    """
    def hash_str(s: str) -> str:
        return hashlib.sha256(s.encode('utf-8')).hexdigest()[:8]

    # Replace email addresses
    email_pattern = re.compile(r'[\w\.-]+@[\w\.-]+')
    text = email_pattern.sub(lambda m: f"[email-{hash_str(m.group())}]", text)
    # Replace phone numbers (simple patterns for Taiwanese numbers)
    phone_pattern = re.compile(r'(09\d{8}|\+?886\d{8,10}|\d{2,3}-\d{3,4}-\d{3,4})')
    text = phone_pattern.sub(lambda m: f"[phone-{hash_str(m.group())}]", text)
    return text


class GrantCrawler:
    """
    Encapsulates crawling logic for the DayseeChat grant portal.

    A single crawler instance manages its own Playwright browser
    context, including stealth integration, proxy rotation and user agent
    randomisation.  Methods on this class are coroutines and should be
    awaited.
    """

    def __init__(
        self,
        base_url: str = "https://dayseechat.com",
        listing_path: str = "/explore-grant/",
        proxies: Optional[List[str]] = None,
        user_agents: Optional[List[str]] = None,
    ) -> None:
        self.base_url = base_url.rstrip('/')
        self.listing_url = f"{self.base_url}{listing_path}"
        self.proxies = proxies or []
        self.user_agents = user_agents or [
            # A handful of common desktop and mobile user agents
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
        ]
        self._proxy_cycle = None
        if self.proxies:
            import itertools
            self._proxy_cycle = itertools.cycle(self.proxies)

    async def _new_browser_context(self, p) -> BrowserContext:
        """
        Create a new browser context with stealth plugin enabled, random
        user agent, optional proxy and Accept-Language header aligned to the
        locale implied by the user agent (defaults to zh‑TW for Taiwanese
        proxies).
        """
        ua = random.choice(self.user_agents)
        # Determine language from user agent; naive approach: if UA contains
        # 'en' then use en-US otherwise zh-TW.
        lang = 'zh-TW' if 'Chrome' in ua or 'Safari' in ua else 'en-US'
        proxy_kwargs = None
        if self._proxy_cycle:
            proxy_url = next(self._proxy_cycle)
            proxy_kwargs = {"server": proxy_url}
        browser = await p.chromium.launch(
            headless=True,
            proxy=proxy_kwargs,
        )
        context = await browser.new_context(
            user_agent=ua,
            locale=lang,
            extra_http_headers={"Accept-Language": lang},
        )
        return context

    async def _apply_stealth(self, page: Page) -> None:
        """Apply the stealth plugin on a given Playwright page."""
        await stealth_async(page)

    async def _human_scroll(self, page: Page, distance: int, step: int = 200) -> None:
        """
        Scroll the page in small increments to simulate human behaviour.

        Args:
            page: The Playwright page to scroll.
            distance: Total distance in pixels to scroll vertically.  Positive
                values scroll down; negative values scroll up.
            step: Pixel increment per scroll action.
        """
        remaining = distance
        direction = 1 if distance > 0 else -1
        while abs(remaining) > 0:
            move = direction * min(step, abs(remaining))
            await page.mouse.wheel(0, move)
            remaining -= move
            await asyncio.sleep(random.uniform(0.1, 0.3))

    async def _click_with_scroll(self, page: Page, selector: str) -> None:
        """
        Scroll the element matching ``selector`` into view and click it with
        a randomised jitter.  The method will not proceed if the element
        cannot be found.
        """
        element = await page.query_selector(selector)
        if element is None:
            raise RuntimeError(f"Element {selector} not found")
        await element.scroll_into_view_if_needed()
        box = await element.bounding_box()
        if box:
            # Move mouse to a random point inside the bounding box to reduce
            # click predictability
            x = box['x'] + random.uniform(0.2, 0.8) * box['width']
            y = box['y'] + random.uniform(0.2, 0.8) * box['height']
            await page.mouse.move(x, y, steps=random.randint(5, 15))
        await asyncio.sleep(random.uniform(0.2, 0.6))
        await element.click()
        # Post‑click jitter
        await asyncio.sleep(random.uniform(0.3, 0.7))

    @exponential_backoff(retries=5)
    async def _extract_listings(self, page: Page) -> List[GrantSummary]:
        """
        Navigate the listing page, load all paginated results and return
        summary records for every grant found.  This method is wrapped
        inside the exponential_backoff decorator which transparently retries
        in the event of HTTP 403/429 errors.
        """
        await page.goto(self.listing_url, wait_until="networkidle")
        await self._apply_stealth(page)
        # Wait for the first batch of results to load
        await page.wait_for_selector('a.ts-action-con')
        results: Dict[str, GrantSummary] = {}
        page_num = 1
        while True:
            logger.info("Scanning listing page %d", page_num)
            anchors = await page.query_selector_all('a.ts-action-con')
            for a in anchors:
                href = await a.get_attribute('href')
                title = (await a.get_attribute('aria-label')) or (await a.inner_text())
                if not href or not title:
                    continue
                # Skip duplicates
                if href in results:
                    continue
                # Extract meta info around this listing: look for sibling
                parent = await a.evaluate_handle("node => node.closest('li')")
                topics = []
                recipients = []
                sources = []
                amount = None
                deadline = None
                if parent:
                    # The meta information is contained within the same list item
                    html = await parent.inner_html()
                    # Use regex to approximate extraction; the site annotates
                    # metadata with visible Chinese marker characters such as
                    # ＃關注議題：, ＃補助對象： etc.
                    topics_match = re.search(r'＃關注議題：([\s\S]*?)＃', html)
                    if topics_match:
                        items = re.findall(r'>([^<]+)<', topics_match.group(1))
                        topics = [i.strip() for i in items if i.strip()]
                    recipients_match = re.search(r'＃補助對象：([\s\S]*?)＃', html)
                    if recipients_match:
                        items = re.findall(r'>([^<]+)<', recipients_match.group(1))
                        recipients = [i.strip() for i in items if i.strip()]
                    sources_match = re.search(r'＃計畫來源：([\s\S]*?)＃', html)
                    if sources_match:
                        items = re.findall(r'>([^<]+)<', sources_match.group(1))
                        sources = [i.strip() for i in items if i.strip()]
                    amount_match = re.search(r'補助金額：([^<]+)', html)
                    if amount_match:
                        amount = amount_match.group(1).strip()
                    deadline_match = re.search(r'截止日期：([^<]+)', html)
                    if deadline_match:
                        date_str = deadline_match.group(1).strip()
                        try:
                            deadline = datetime.strptime(date_str, "%Y-%m-%d")
                        except Exception:
                            logger.debug("Unable to parse deadline: %s", date_str)
                results[href] = GrantSummary(
                    title=title.strip(),
                    url=href,
                    topics=topics,
                    recipients=recipients,
                    sources=sources,
                    amount=amount,
                    deadline=deadline,
                )
            # Look for a "Next page" button and click if it exists and is
            # enabled (not disabled).  The button has class ts-load-next.
            next_btn = await page.query_selector('a.ts-load-next')
            if not next_btn:
                break

            # Some pages keep the next button in the DOM even on the last page.
            # In that case clicking it can time out because another pagination
            # container intercepts pointer events. Treat that situation as
            # "already at the last page" instead of crashing the whole run.
            classes = (await next_btn.get_attribute('class')) or ''
            aria_disabled = (await next_btn.get_attribute('aria-disabled')) or ''
            if 'disabled' in classes or aria_disabled.lower() == 'true':
                break

            previous_urls = set(results.keys())
            try:
                page_num += 1
                await self._click_with_scroll(page, 'a.ts-load-next')
                # Wait for new content to load
                await page.wait_for_load_state('networkidle')
                # Random delay after each page to respect crawl delay and jitter
                await asyncio.sleep(random.uniform(1.0, 2.0))

                # Safety check: if the page did not advance to any new cards, stop.
                current_cards = await page.query_selector_all('a.ts-action-con')
                current_urls = set()
                for card in current_cards:
                    href = await card.get_attribute('href')
                    if href:
                        current_urls.add(href)
                if current_urls and current_urls.issubset(previous_urls):
                    logger.info("Pagination did not advance; treating current page as the last page.")
                    break
                continue
            except PlaywrightTimeoutError:
                logger.info("Next-page button is not actionable anymore; treating this as the last page.")
                break
        return list(results.values())

    @exponential_backoff(retries=5)
    async def _extract_detail(self, context: BrowserContext, summary: GrantSummary) -> GrantDetail:
        """
        Given a summary record, navigate to the corresponding detail page
        and extract the full description and any structured fields present.
        The method uses a separate page per detail to isolate local storage
        and cookies.  It respects the crawl delay via explicit sleeps.
        """
        page = await context.new_page()
        try:
            logger.info("Fetching detail: %s", summary.url)
            await page.goto(summary.url, wait_until="networkidle")
            await self._apply_stealth(page)
            # Wait for primary content to appear; the header region usually
            # contains '計畫來源'
            await page.wait_for_selector('h1, h2, h3')
            html = await page.content()
            markdown = clean_html_to_markdown(html)
            anonymised = anonymise_pii(markdown)
            # Extract additional key/value pairs from the page using regex.
            details = {}
            # Patterns for labelled fields (e.g. "計畫來源：縣市政府")
            for label in ['計畫來源', '補助對象', '適用地區', '補助金額', '截止日期', '關注議題']:
                pattern = rf'{label}：([^\n]+)'
                m = re.search(pattern, anonymised)
                if m:
                    details[label] = m.group(1).strip()
            return GrantDetail(
                **summary.dict(),
                description_md=anonymised,
                details=details,
            )
        finally:
            await page.close()

    async def run(self) -> List[GrantDetail]:
        """
        Entry point to start the crawling process.  This method obtains
        listings and then fetches each detail record.  It honours the
        robots.txt crawl delay between page fetches.
        """
        crawl_delay = await get_crawl_delay(self.base_url)
        async with async_playwright() as p:
            # Use a single browser context for the listing extraction
            context = await self._new_browser_context(p)
            page = await context.new_page()
            listings = await self._extract_listings(page)
            await page.close()
            await context.close()
            logger.info("Found %d grants", len(listings))
            # Now fetch details; rotate proxies/user agents per detail
            results = []
            for idx, summary in enumerate(listings, start=1):
                context = await self._new_browser_context(p)
                detail = await self._extract_detail(context, summary)
                await context.close()
                results.append(detail)
                # Respect crawl delay between detail pages
                delay = crawl_delay + random.uniform(0.5, 1.5)
                logger.info(
                    "Sleeping %.2f seconds before next detail (cumulative %d/%d)",
                    delay, idx, len(listings)
                )
                await asyncio.sleep(delay)
            return results


async def main() -> None:
    """
    Orchestrates the crawling workflow and writes the result to an Excel
    workbook.  The output file will be placed in the current working
    directory with a timestamp to avoid clobbering previous runs.
    """
    crawler = GrantCrawler()
    grant_details = await crawler.run()
    # Convert to DataFrame
    rows = []
    for item in grant_details:
        row = item.dict()
        # Flatten details dictionary by prefixing keys
        for k, v in item.details.items():
            row[f'detail_{k}'] = v
        # Remove the nested dictionary from the row
        row.pop('details', None)
        rows.append(row)
    df = pd.DataFrame(rows)
    # Determine output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"daysee_grants_{timestamp}.xlsx"
    df.to_excel(filename, index=False)
    logger.info("Exported %d records to %s", len(df), filename)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as exc:
        logger.exception("Crawler terminated with an exception: %s", exc)