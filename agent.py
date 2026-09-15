"""
Xetarev Crawlspace
-----------------------
Runs on GitHub Actions on a cron schedule.
Pipeline:
  1. Poll RSS feeds for new articles (weighted by historical Gemini skip rates)
  2. Filter unseen articles using seen_articles.json
  3. Title-first relevance gate via Gemini (title + source only; skip off-topic)
  4. Fetch full article text via trafilatura (only if relevant)
  5. Prefer featured image from RSS feed data; DuckDuckGo as fallback (skip if none)
  6. Generate SEO post via Gemini (title, slug, excerpt, body)
  7. Build Payload Lexical JSON content structure
  8. POST to Payload CMS REST API as published
  9. Update per-feed skip/publish stats in feed_quality.json
 10. Commit updated seen_articles.json and feed_quality.json back to repo
"""
import os
import json
import time
import copy
import hashlib
import logging
import random
import re
import ssl
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import httpx
import feedparser
import trafilatura
from dotenv import load_dotenv
from google import genai
from google.genai import types
from slugify import slugify
from datetime import datetime, timezone
from texthumanize import humanize

# Load .env when present (local runs). GitHub Actions already injects env vars;
# load_dotenv does not override existing environment variables by default.
load_dotenv()

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)
# ─── Dry-run diagnostics (stdout, separate from normal logging) ───────────────
_dry_print_lock = threading.Lock()


def dry_print(label: str, content: str = "") -> None:
    """Print clearly labeled dry-run output so it stands out in the terminal."""
    sep = "─" * 72
    with _dry_print_lock:
        print(f"\n{sep}")
        print(f"[DRY-RUN] {label}")
        print(sep)
        if content:
            print(content)
# ─── Config from env / GitHub Actions Secrets ───────────────────────────────
GEMINI_API_KEY    = os.environ["GEMINI_API_KEY"]
PAYLOAD_API_URL   = os.environ["PAYLOAD_API_URL"]   # e.g. https://your-app.vercel.app/api/inlight-posts
PAYLOAD_API_TOKEN = os.environ["PAYLOAD_API_TOKEN"]
# ─── RSS Feeds to poll ──────────────────────────────────────────────────────
# Add or remove feeds here. These are all verified active in 2026.
RSS_FEEDS = [
    # Mainstream Tech News & Analysis
    ("TechCrunch","https://techcrunch.com/feed/"),
    ("WIRED","https://www.wired.com/feed/rss"),
    ("The Verge","https://www.theverge.com/rss/index.xml"),
    ("ZDNET","https://www.zdnet.com/news/rss.xml"),
    ("Gizmodo","https://gizmodo.com/rss"),
    ("Hugging Face","https://huggingface.co/blog/feed.xml"),
    ("OpenAI News","https://openai.com/news/rss.xml"),
    ("Google DeepMind","https://deepmind.google/blog/feed"),
    ("The Decoder","https://the-decoder.com/feed"),
    ("NVIDIA Developer","https://developer.nvidia.com/blog//feed"),
    ("CNET","https://www.cnet.com/rss/news/"),
    ("PCMag","https://www.pcmag.com/feeds/rss/latest"),
    ("Tom's Hardware","https://www.tomshardware.com/feeds.xml"),
    ("TechRadar","https://www.techradar.com/feeds.xml"),
    ("GSMArena","https://www.gsmarena.com/rss-news-reviews.php3"),
    ("XDA","https://www.xda-developers.com/feed"),
    ("Android Authority","https://www.androidauthority.com/feed"),
    ("9to5Google", "https://9to5google.com/feed"),
    ("Android Police","https://www.androidpolice.com/feed"),
    ("SamMobile","https://www.sammobile.com/feed"),
    ("Y Combinator Hacker News","https://news.ycombinator.com/rss"),
    ("9to5Mac","https://9to5mac.com/feed"),
    ("MacRumors","https://feeds.macrumors.com/MacRumors-All"),
    ("AppleInsider","https://appleinsider.com/rss/news"),
    ("Macworld","https://www.macworld.com/index.rss"),
    ("MIT Technology Review","https://www.technologyreview.com/feed"),
    ("IEEE Spectrum","https://feeds.feedburner.com/IeeeSpectrum"),
    ("O'Reilly","https://feeds.feedburner.com/oreilly/radar"),
    ("VentureBeat", "https://feeds.feedburner.com/venturebeat/SZYF"),
    ("Ars Technica","https://feeds.arstechnica.com/arstechnica/index"),
    ("Futurism","https://futurism.com/feed"),
    ("MakeUseOf","https://www.makeuseof.com/feed/category/technology-explained/"),
    ("How-To Geek","https://www.howtogeek.com/feed/"),
    # AI & Machine Learning
    # Consumer Tech & Gadget Reviews
    # Mobile & Smartphone News
    # Cybersecurity
    # Software Developer & Programming
    # Apple Ecosystem
    # Enterprise, IT & Cloud
    # Startups, Venture & Business of Tech
    # Culture, Opinion & Analysis
    # Tutorials, How-To & Explainers
    # Science & Emerging Tech
    # ("Engadget","https://www.engadget.com/rss.xml"),
    # ("Digital Trends","https://www.digitaltrends.com/feed"),
    # ("Android Central","https://feeds.feedburner.com/androidcentral"),
    # ("SlashGear","https://www.slashgear.com/feed"),
    # ("Tom's Guide","https://www.tomsguide.com/feeds.xml"),
    # ("Computerworld","https://www.computerworld.com/feed"),
    # ("InfoWorld","https://www.infoworld.com/feed"),
    # ("SiliconANGLE","https://siliconangle.com/feed"),
    # ("The Register","https://search.theregister.com/?q=technology&_t=all&feed=1"),
    # ("Synced","https://syncedreview.com/feed"),
    # ("KDnuggets","https://www.kdnuggets.com/feed"),
    # ("Towards Data Science","https://towardsdatascience.com/feed"),
    # ("Unite.AI","https://www.unite.ai/feed/"),
    # ("MarkTechPost","https://www.marktechpost.com/feed/"),
    # ("TechSpot","https://www.techspot.com/backend.xml"),
    # ("Trusted Reviews","https://www.trustedreviews.com/feed"),
    # ("Android Headlines","https://www.androidheadlines.com/feed"),
    # ("Pocketnow","https://pocketnow.com/feed"),
    # ("Krebs on Security","https://krebsonsecurity.com/feed"),
    # ("The Hacker News","https://feeds.feedburner.com/TheHackersNews"),
    # ("BleepingComputer","https://www.bleepingcomputer.com/feed"),
    # ("Dark Reading","https://www.darkreading.com/rss.xml"),
    # ("GitHub Blog","https://github.blog/feed"),
    # ("SecurityWeek","https://www.securityweek.com/feed"),
    # ("CSO Online", "https://www.csoonline.com/feed"),
    # ("Help Net Security","https://www.helpnetsecurity.com/feed"),
    # ("Schneier on Security","https://www.schneier.com/feed"),
    # ("Sophos","https://news.sophos.com/en-us/feed/"),
    # ("PhoneArena", "https://www.phonearena.com/feed/news"),
    # ("TechRepublic","https://www.techrepublic.com/rssfeeds/articles/"),
    # ("Tech.eu","https://tech.eu/feed"),
    # ("GeekWire","https://geekwire.com/feed"),
    # ("Silicon Republic","https://www.siliconrepublic.com/feed"),
    # ("Stack Overflow Blog","https://stackoverflow.blog/feed"),
    # ("InfoQ","https://feed.infoq.com"),
    # ("The New Stack","https://thenewstack.io/feed"),
    # ("Smashing Magazine","https://www.smashingmagazine.com/feed"),
    # ("CSS-Tricks", "https://css-tricks.com/feed"),
    # ("freeCodeCamp","https://www.freecodecamp.org/news/rss"),
    # ("DEV Community","https://dev.to/feed"),
    # ("SitePoint","https://www.sitepoint.com/sitepoint.rss"),
    # ("iMore","https://www.imore.com/rss.xml"),
    # ("iLounge","https://www.ilounge.com/feed"),
    # ("Vox Technology","https://www.vox.com/rss/technology/index.xml"),
    # ("The Next Web","https://feeds.feedburner.com/thenextweb"),
    # ("Slashdot","https://rss.slashdot.org/Slashdot/slashdotMain"),
    # ("Techdirt","https://feeds.feedburner.com/techdirt"),
    # ("Firstpost Tech","https://www.firstpost.com/commonfeeds/v1.0/tech.xml"),
    # ("HuffPost Tech","https://www.huffpost.com/section/technology/feed"),
    # ("gHacks","https://www.ghacks.net/feed"),
    # ("Fossbytes","https://fossbytes.com/feed/?x=1"),
    # ("BetaNews","https://betanews.com/feed/"),
    # ("New Scientist","https://www.newscientist.com/feed/home/"),
    # ("Quanta Magazine","https://www.quantamagazine.org/feed"),
    # ("Live Science","https://www.livescience.com/feeds/all"),
    # ("Phys.org","https://phys.org/rss-feed/physics-news"),
    # ("Tech Xplore","https://techxplore.com/rss-feed"),
    # ("ScienceDaily","https://www.sciencedaily.com/rss/all.xml"),
    # ("Interesting Engineering","https://interestingengineering.com/feed"),
]
# ─── Agent settings ──────────────────────────────────────────────────────────
ARTICLES_PER_RUN   = 4    # articles fetched per batch
MIN_PUBLISH        = ARTICLES_PER_RUN // 2  # publish at least 50% of batch size per run
STATE_FILE         = "seen_articles.json"
FEED_STATS_FILE    = "feed_quality.json"
# Soft-deprioritize feeds once they have enough Gemini topic decisions.
# Weight never hits zero — chronically weak feeds stay eligible, just less often.
FEED_STATS_MIN_SAMPLES = 3
FEED_STATS_MIN_WEIGHT  = 0.15
MIN_SUMMARY_LENGTH = 300   # chars — below this we try trafilatura for full text
# Stage 4 AI-audit: scores at or above this are not "Likely Human" enough to post as-is
HUMAN_SCORE_THRESHOLD = 35
MAX_HUMANIZE_RETRIES = 2
# Transient HTTP retries (timeouts, connection errors, 5xx)
HTTP_MAX_ATTEMPTS = 3
# Payload slug-check GET + publish POST: higher ceiling — a lost POST drops the article.
PAYLOAD_HTTP_MAX_ATTEMPTS = 5
# Distinct ladders: short for transient faults; long for rate limits (429).
HTTP_TRANSIENT_BACKOFF_SEC = (1.0, 2.0, 4.0)
HTTP_RATE_LIMIT_BACKOFF_SEC = (15.0, 30.0, 60.0)
# Uniform jitter as a fraction of the chosen base delay (±), all retry cases.
HTTP_RETRY_JITTER_FRAC = 0.25
MODEL_CHAIN = [
    "gemini-3.5-flash-lite",
    "gemini-3.1-flash-lite",
    "gemini-3.5-flash",
    "gemini-3.6-flash",
    "gemini-3.7-flash",
]
# ─── Transient HTTP retries ───────────────────────────────────────────────────
class TransientHTTPError(Exception):
    """Timeout, connection error, or retryable HTTP status (may carry 429 metadata)."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after: float | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


class NonRetryableHTTPError(Exception):
    """Permanent failures (e.g. SSL/certificate) that must not use the retry ladder."""


def _is_ssl_cert_error(exc: BaseException | None) -> bool:
    """True for certificate verify / expired / SSL errors that won't heal on retry."""
    if exc is None:
        return False
    if isinstance(exc, ssl.SSLError):
        return True
    reason = getattr(exc, "reason", None)
    if isinstance(reason, BaseException) and _is_ssl_cert_error(reason):
        return True
    cause = getattr(exc, "__cause__", None) or getattr(exc, "__context__", None)
    if isinstance(cause, BaseException) and cause is not exc and _is_ssl_cert_error(cause):
        return True
    msg = str(exc).lower()
    return (
        "certificate_verify_failed" in msg
        or "certificate verify failed" in msg
        or "certificate has expired" in msg
        or "ssl: certificate" in msg
        or "sslcertverificationerror" in msg
    )


def _is_retryable_http_status(status_code: int) -> bool:
    return status_code in (408, 425, 429) or status_code >= 500


def _is_rate_limit_status(status_code: int | None) -> bool:
    return status_code == 429


def _parse_retry_after_seconds(value: str | None) -> float | None:
    """Parse Retry-After as delta-seconds or HTTP-date; None if missing/invalid."""
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        from email.utils import parsedate_to_datetime

        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError, OverflowError, IndexError):
        return None


def _jittered_delay(base_sec: float) -> float:
    """Apply ±HTTP_RETRY_JITTER_FRAC uniform jitter so concurrent retries desync."""
    if base_sec <= 0:
        return 0.0
    span = base_sec * HTTP_RETRY_JITTER_FRAC
    return max(0.0, base_sec + random.uniform(-span, span))


def _backoff_delay(
    attempt: int,
    *,
    status_code: int | None = None,
    retry_after: float | None = None,
) -> float:
    """
    Seconds to wait after a failed attempt (1-based).

    Prefer Retry-After when the server sent one; otherwise use the rate-limit
    ladder (15/30/60) for 429 and the short transient ladder (1/2/4) for
    timeouts / 5xx / other retryable faults. Jitter is applied in every case.
    """
    if retry_after is not None:
        return _jittered_delay(retry_after)
    ladder = (
        HTTP_RATE_LIMIT_BACKOFF_SEC
        if _is_rate_limit_status(status_code)
        else HTTP_TRANSIENT_BACKOFF_SEC
    )
    idx = min(max(attempt, 1) - 1, len(ladder) - 1)
    return _jittered_delay(ladder[idx])


def call_with_http_retries(operation, *, label: str, max_attempts: int = HTTP_MAX_ATTEMPTS):
    """
    Run operation() with exponential backoff on TransientHTTPError.
    429 uses a longer ladder than timeouts/5xx; Retry-After wins when present.
    NonRetryableHTTPError and other non-transient exceptions propagate immediately.
    """
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except NonRetryableHTTPError:
            raise
        except TransientHTTPError as e:
            last_error = e
            if attempt >= max_attempts:
                break
            delay = _backoff_delay(
                attempt,
                status_code=e.status_code,
                retry_after=e.retry_after,
            )
            kind = (
                "rate limit (429)"
                if _is_rate_limit_status(e.status_code)
                else "transient failure"
            )
            retry_after_note = (
                f", Retry-After={e.retry_after:.1f}s" if e.retry_after is not None else ""
            )
            log.warning(
                f"{label}: {kind} "
                f"(attempt {attempt}/{max_attempts}): {e}{retry_after_note}; "
                f"retrying in {delay:.1f}s"
            )
            time.sleep(delay)
    raise TransientHTTPError(
        f"{label}: exhausted {max_attempts} attempts — {last_error}",
        status_code=getattr(last_error, "status_code", None),
        retry_after=getattr(last_error, "retry_after", None),
    ) from last_error


def http_request(
    method: str,
    url: str,
    *,
    label: str,
    retryable_statuses: bool = True,
    max_attempts: int = HTTP_MAX_ATTEMPTS,
    **kwargs,
) -> httpx.Response:
    """httpx.request with retries on timeouts, network errors, and 5xx/429."""

    def _do():
        try:
            response = httpx.request(method, url, **kwargs)
        except (
            httpx.TimeoutException,
            httpx.NetworkError,
            httpx.RemoteProtocolError,
        ) as e:
            if _is_ssl_cert_error(e):
                raise NonRetryableHTTPError(f"SSL/certificate error: {e}") from e
            raise TransientHTTPError(str(e)) from e
        if retryable_statuses and _is_retryable_http_status(response.status_code):
            retry_after = _parse_retry_after_seconds(
                response.headers.get("Retry-After")
            )
            raise TransientHTTPError(
                f"HTTP {response.status_code}",
                status_code=response.status_code,
                retry_after=retry_after,
            )
        return response

    return call_with_http_retries(_do, label=label, max_attempts=max_attempts)


def parse_feed_with_retries(feed_url: str):
    """feedparser.parse with retries on transient HTTP / network failures.
    SSL/certificate errors fail immediately (no backoff ladder).
    """

    def _do():
        feed = feedparser.parse(feed_url)
        status = getattr(feed, "status", None)
        if isinstance(status, int) and _is_retryable_http_status(status):
            raise TransientHTTPError(
                f"feed HTTP {status}",
                status_code=status,
            )
        # Network failures often surface as bozo + empty entries
        if getattr(feed, "bozo", False) and not getattr(feed, "entries", None):
            bozo_exc = getattr(feed, "bozo_exception", None)
            if bozo_exc is not None:
                if _is_ssl_cert_error(bozo_exc):
                    raise NonRetryableHTTPError(
                        f"SSL/certificate error: {bozo_exc}"
                    ) from bozo_exc
                raise TransientHTTPError(str(bozo_exc)) from bozo_exc
        return feed

    return call_with_http_retries(_do, label=f"RSS fetch {feed_url}")


def fetch_url_with_retries(url: str) -> str:
    """
    trafilatura.fetch_url with retries.
    Raises TransientHTTPError when all attempts return nothing or error.
    SSL/certificate errors fail immediately.
    """

    def _do():
        try:
            downloaded = trafilatura.fetch_url(url)
        except Exception as e:
            if _is_ssl_cert_error(e):
                raise NonRetryableHTTPError(f"SSL/certificate error: {e}") from e
            raise TransientHTTPError(str(e)) from e
        if not downloaded:
            raise TransientHTTPError("empty response / fetch failed")
        return downloaded

    return call_with_http_retries(_do, label=f"trafilatura {url}")


# ─── Xetarev brand context injected into every Gemini prompt ────────────────
BRAND_CONTEXT = """
You write for Xetarev Inlight — an indie tech company's blog.
About the company (context only — do not plug it in posts):
- Indie tech shop: ships its own products (Apps) and selective client work (Studio)
- Roots for builders, small teams, and craft over hype
Voice:
- Write like a knowledgeable person with a point of view, not a content pipeline
- First person where it feels natural ("I think…", "here's what matters…")
- Direct opinions. Sharp and informed. Human.
- Skeptical of hype; call out spin. Respect good engineering.
- No corporate press-release tone. No generic AI-blog filler.
Burstiness (write like a human, not a uniform model):
- When it comes to writing content, two factors are crucial — perplexity and burstiness. Perplexity measures the complexity of text. Separately, burstiness compares the variations of sentences. Humans tend to write with greater burstiness, for example, with some longer or complex sentences alongside shorter ones. AI sentences tend to be more uniform.
- Mix short punchy sentences with longer, winding ones. Vary rhythm on purpose. Avoid even sentence lengths.
"""
# ─── State management ────────────────────────────────────────────────────────
def load_seen() -> set:
    """Load the set of already-processed article IDs from disk."""
    try:
        with open(STATE_FILE, "r") as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()
def save_seen(seen: set) -> None:
    """Persist the updated seen set to disk."""
    with open(STATE_FILE, "w") as f:
        json.dump(sorted(list(seen)), f, indent=2)
    log.info(f"Saved {len(seen)} seen article IDs to {STATE_FILE}")
def load_feed_stats() -> dict:
    """Load per-feed Gemini skip/publish counters from disk."""
    try:
        with open(FEED_STATS_FILE, "r") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
def save_feed_stats(stats: dict) -> None:
    """Persist per-feed quality counters to disk."""
    with open(FEED_STATS_FILE, "w") as f:
        json.dump(stats, f, indent=2, sort_keys=True)
    log.info(f"Saved feed quality stats for {len(stats)} feed(s) to {FEED_STATS_FILE}")
def feed_selection_weight(stats_entry: dict | None) -> float:
    """
    Selection weight from historical Gemini topic decisions.
    Neutral (1.0) until FEED_STATS_MIN_SAMPLES decisions; then inverse skip rate
    floored at FEED_STATS_MIN_WEIGHT so feeds are never removed entirely.
    """
    if not stats_entry:
        return 1.0
    skipped = int(stats_entry.get("skipped", 0) or 0)
    published = int(stats_entry.get("published", 0) or 0)
    total = skipped + published
    if total < FEED_STATS_MIN_SAMPLES:
        return 1.0
    skip_rate = skipped / total
    return max(FEED_STATS_MIN_WEIGHT, 1.0 - skip_rate)
def record_feed_outcome(
    stats: dict,
    feed_url: str,
    source_name: str,
    outcome: str,
) -> None:
    """
    Increment skipped or published counter for a feed URL.

    Content decisions only:
      - "skipped" — title-gate rejection (or Gemini {"skip": true})
      - "published" — generate_post returned a valid on-topic post
    Never call this for Payload/CMS infrastructure outcomes.
    """
    if not feed_url or outcome not in ("skipped", "published"):
        return
    entry = stats.setdefault(
        feed_url,
        {"name": source_name, "skipped": 0, "published": 0},
    )
    entry["name"] = source_name or entry.get("name") or feed_url
    entry[outcome] = int(entry.get(outcome, 0) or 0) + 1
def article_id(entry) -> str:
    """Stable unique ID for a feed entry — prefer guid, fallback to URL hash."""
    raw = getattr(entry, "id", None) or getattr(entry, "link", "")
    return hashlib.md5(raw.encode()).hexdigest()
# ─── RSS polling ─────────────────────────────────────────────────────────────
def _looks_like_image_url(url: str) -> bool:
    if not url or not url.startswith(("http://", "https://")):
        return False
    path = url.split("?", 1)[0].lower()
    return any(path.endswith(ext) for ext in (
        ".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".svg",
    )) or "/image" in path or "img" in path
def extract_feed_image(entry) -> str | None:
    """
    Pull a featured image URL from feedparser entry fields when present.
    Checks media_content, media_thumbnail, enclosure, and links.
    """
    # media_content: list of dicts with 'url' / 'type' / 'medium'
    for media in entry.get("media_content") or []:
        url = (media.get("url") or "").strip()
        if not url:
            continue
        media_type = (media.get("type") or "").lower()
        medium = (media.get("medium") or "").lower()
        if medium == "image" or media_type.startswith("image/") or _looks_like_image_url(url):
            return url
        # Some feeds omit type/medium but still put the hero image here
        if not media_type and not medium:
            return url
    # media_thumbnail: list of dicts with 'url'
    for thumb in entry.get("media_thumbnail") or []:
        url = (thumb.get("url") or "").strip()
        if url:
            return url
    # enclosure / enclosures
    enclosures = entry.get("enclosures") or []
    if not enclosures and entry.get("enclosure"):
        enclosures = [entry.get("enclosure")]
    for enc in enclosures:
        if not isinstance(enc, dict):
            continue
        url = (enc.get("href") or enc.get("url") or "").strip()
        enc_type = (enc.get("type") or "").lower()
        if url and (enc_type.startswith("image/") or _looks_like_image_url(url)):
            return url
    # links with image type or rel=enclosure / image / thumbnail
    for link in entry.get("links") or []:
        if not isinstance(link, dict):
            continue
        url = (link.get("href") or "").strip()
        if not url:
            continue
        link_type = (link.get("type") or "").lower()
        rel = (link.get("rel") or "").lower()
        if link_type.startswith("image/") or rel in ("image", "thumbnail"):
            return url
        if rel == "enclosure" and (link_type.startswith("image/") or _looks_like_image_url(url)):
            return url
    return None
def _weighted_sample_feeds(
    available: list[tuple[str, str]],
    feed_stats: dict,
    k: int,
) -> list[tuple[str, str]]:
    """
    Sample up to k feeds without replacement, biased by feed_selection_weight.
    Low-quality feeds remain in the pool at FEED_STATS_MIN_WEIGHT.
    """
    if not available or k <= 0:
        return []
    pool = list(available)
    selected: list[tuple[str, str]] = []
    for _ in range(min(k, len(pool))):
        weights = [feed_selection_weight(feed_stats.get(url)) for _, url in pool]
        # random.choices requires positive weights; guard against bad persisted data
        weights = [w if w > 0 else FEED_STATS_MIN_WEIGHT for w in weights]
        idx = random.choices(range(len(pool)), weights=weights, k=1)[0]
        selected.append(pool.pop(idx))
    return selected
def fetch_new_articles(
    seen: set,
    used_feeds: set | None = None,
    feed_stats: dict | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """
    Weight-sample unused feeds by historical Gemini skip rates, poll up to
    ARTICLES_PER_RUN of them, return one unseen article per feed.
    used_feeds tracks feed URLs already polled this run.
    """
    used = used_feeds if used_feeds is not None else set()
    stats = feed_stats if feed_stats is not None else {}
    available = [(name, url) for name, url in RSS_FEEDS if url not in used]
    if not available:
        log.info("No unused feeds left to poll")
        if dry_run:
            dry_print("Feed selection", "No unused feeds left to poll")
        return []

    selected = _weighted_sample_feeds(available, stats, ARTICLES_PER_RUN)
    weight_notes = []
    for name, url in selected:
        w = feed_selection_weight(stats.get(url))
        entry = stats.get(url) or {}
        skipped = int(entry.get("skipped", 0) or 0)
        published = int(entry.get("published", 0) or 0)
        weight_notes.append(f"{name} (w={w:.2f}, skip={skipped}, pub={published})")
    log.info(f"Selected {len(selected)} feeds this batch: {weight_notes}")
    if dry_run:
        lines = [
            f"  - {name} ({url}) weight={feed_selection_weight(stats.get(url)):.2f}"
            for name, url in selected
        ]
        dry_print("Feeds selected this batch", "\n".join(lines) if lines else "(none)")

    candidates: list[dict] = []
    # Mark selected feeds used up front so parallel workers don't double-poll.
    for _, feed_url in selected:
        used.add(feed_url)

    def _poll_one(source_name: str, feed_url: str) -> dict | None:
        try:
            feed = parse_feed_with_retries(feed_url)
            for entry in feed.entries:
                uid = article_id(entry)
                if uid in seen:
                    continue
                feed_image = extract_feed_image(entry)
                return {
                    "id":       uid,
                    "source":   source_name,
                    "feed_url": feed_url,
                    "title":    entry.get("title", "").strip(),
                    "url":      entry.get("link", "").strip(),
                    "summary":  entry.get("summary", "").strip(),
                    "image":    feed_image,
                }
            return None
        except NonRetryableHTTPError as e:
            log.warning(f"Failed to fetch feed {feed_url} (non-retryable): {e}")
            return None
        except TransientHTTPError as e:
            log.warning(f"Failed to fetch feed after retries {feed_url}: {e}")
            return None
        except Exception as e:
            log.warning(f"Failed to fetch feed {feed_url}: {e}")
            return None

    workers = min(8, len(selected)) if selected else 1
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(_poll_one, source_name, feed_url)
            for source_name, feed_url in selected
        ]
        for fut in as_completed(futures):
            article = fut.result()
            if article:
                candidates.append(article)

    log.info(f"Found {len(candidates)} unseen articles across selected feeds")
    return candidates
# ─── Full text extraction ─────────────────────────────────────────────────────
def get_article_text(article: dict, dry_run: bool = False) -> str:
    """
    Return the best available text for an article.
    Uses RSS summary if long enough, otherwise fetches full text via trafilatura
    (with exponential-backoff retries).
    Falls back to summary if trafilatura fails or is blocked.
    Raises TransientHTTPError when the body HTTP fetch fails after retries and
    there is no usable RSS summary fallback (caller must not mark the article seen).
    """
    summary = article.get("summary", "")
    # Strip HTML tags from summary if present
    clean_summary = re.sub(r"<[^>]+>", "", summary).strip()
    if len(clean_summary) >= MIN_SUMMARY_LENGTH:
        log.info(f"Using RSS summary for: {article['title'][:60]}")
        text = clean_summary
    else:
        log.info(f"Summary too short ({len(clean_summary)} chars), fetching full text: {article['url']}")
        text = None
        transient_fetch_failed = False
        try:
            downloaded = fetch_url_with_retries(article["url"])
            extracted = trafilatura.extract(
                downloaded,
                include_formatting=False,
                include_comments=False,
                no_fallback=False,
            )
            if extracted and len(extracted) > len(clean_summary):
                log.info(f"trafilatura extracted {len(extracted)} chars")
                text = extracted[:4000]  # cap to avoid burning Gemini tokens
        except NonRetryableHTTPError as e:
            log.warning(f"trafilatura non-retryable failure for {article['url']}: {e}")
        except TransientHTTPError as e:
            transient_fetch_failed = True
            log.warning(f"trafilatura failed after retries for {article['url']}: {e}")
        if text is None:
            if transient_fetch_failed and not clean_summary:
                # Transient body fetch with no fallback — keep eligible for a later run
                raise TransientHTTPError(
                    f"no article body after retries and empty RSS summary: {article['url']}"
                )
            log.info("Falling back to RSS summary")
            text = clean_summary or article.get("title", "")
    if dry_run:
        preview = (text[:120] + "…") if text and len(text) > 120 else (text or "(empty)")
        dry_print(
            "Extracted article text (before Gemini)",
            f"chars: {len(text) if text else 0}\npreview: {preview}",
        )
    return text
# ─── Image search ─────────────────────────────────────────────────────────────
_IMAGE_QUERY_STOPWORDS = frozenset({
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "as", "by", "with", "from", "into", "over", "after", "before",
    "is", "are", "was", "were", "be", "been", "being", "has", "have", "had",
    "do", "does", "did", "will", "would", "could", "should", "may", "might",
    "this", "that", "these", "those", "it", "its", "new", "how", "what",
    "when", "where", "why", "who", "which", "about", "into", "via",
})


def is_image_accessible(url: str) -> bool:
    """Return True if a HEAD request to url returns status 200."""
    try:
        r = httpx.head(
            url,
            timeout=5,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; Xetarev-Agent/1.0)",
                # Mimic browser load on the published post so hotlink-protected
                # CDNs return 403 here instead of a false 200.
                "Referer": "https://xetarev.com",
            },
        )
        return r.status_code == 200
    except Exception:
        return False


def check_images_accessible(urls: list[str]) -> dict[str, bool]:
    """
    HEAD-check multiple image URLs concurrently.
    Returns a map of url -> accessible for each distinct non-empty URL.
    """
    unique: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        unique.append(url)
    if not unique:
        return {}
    if len(unique) == 1:
        return {unique[0]: is_image_accessible(unique[0])}

    results: dict[str, bool] = {}
    workers = min(8, len(unique))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(is_image_accessible, url): url for url in unique}
        for fut in as_completed(futures):
            url = futures[fut]
            try:
                results[url] = fut.result()
            except Exception:
                results[url] = False
    return results


def _image_fallback_words(query: str) -> list[str]:
    """
    Extract up to two distinct, relevant single words from query for DDG retries.
    Prefers longer non-stopword tokens (more specific search terms).
    """
    tokens = re.findall(r"[A-Za-z][A-Za-z0-9+-]*", query or "")
    seen: set[str] = set()
    ranked: list[tuple[int, str]] = []
    for tok in tokens:
        key = tok.lower()
        if key in _IMAGE_QUERY_STOPWORDS or key in seen or len(key) < 3:
            continue
        seen.add(key)
        ranked.append((len(key), tok))
    # Longer first (more distinctive); stable enough for retries
    ranked.sort(key=lambda item: (-item[0], item[1].lower()))
    return [word for _, word in ranked[:2]]


def _ddg_search_accessible_images(
    query: str,
    exclude: set[str],
    count: int = 1,
) -> list[str]:
    """
    Run one DDG image search; return up to `count` distinct accessible URLs
    not in exclude (earliest results preferred).
    """
    if count <= 0:
        return []
    search_url = "https://duckduckgo.com/"
    r = httpx.get(search_url, params={"q": query}, timeout=10, headers={
        "User-Agent": "Mozilla/5.0 (compatible; Xetarev-Agent/1.0)"
    })
    vqd = re.search(r'vqd=([\d-]+)', r.text)
    if not vqd:
        raise ValueError("Could not extract vqd token")
    img_url = "https://duckduckgo.com/i.js"
    r2 = httpx.get(img_url, params={
        "q": query,
        "vqd": vqd.group(1),
        "f": ",,,,,",
        "p": "1",
    }, timeout=10, headers={
        "User-Agent": "Mozilla/5.0 (compatible; Xetarev-Agent/1.0)",
        "Referer": "https://duckduckgo.com/",
    })
    results = r2.json().get("results", [])
    candidates: list[str] = []
    seen_local: set[str] = set(exclude)
    for result in results:
        image = result.get("image", "")
        if not image or image in seen_local:
            continue
        seen_local.add(image)
        candidates.append(image)
    found: list[str] = []
    batch_size = 6
    for i in range(0, len(candidates), batch_size):
        if len(found) >= count:
            break
        batch = candidates[i : i + batch_size]
        access = check_images_accessible(batch)
        for image in batch:
            if access.get(image):
                found.append(image)
                if len(found) >= count:
                    break
    return found


def _ddg_search_accessible_image(
    query: str,
    exclude: set[str],
) -> str | None:
    """Run one DDG image search; return first accessible URL not in exclude."""
    found = _ddg_search_accessible_images(query, exclude, count=1)
    return found[0] if found else None


def find_image_url(
    query: str,
    exclude_urls: set[str] | None = None,
    dry_run: bool = False,
    dry_label: str = "Image search",
) -> str | None:
    """
    Search DuckDuckGo Images for a relevant image URL.
    Tries each result until one is accessible via HEAD.
    Skips any URL in exclude_urls (e.g. already chosen as featured).

    If the original query yields nothing, retries up to twice with single-word
    fallbacks drawn from the query (most relevant word, then a different one).
    Returns None only if all three attempts come back empty.
    """
    found = find_image_urls(
        query,
        count=1,
        exclude_urls=exclude_urls,
        dry_run=dry_run,
        dry_label=dry_label,
    )
    return found[0] if found else None


def find_image_urls(
    query: str,
    *,
    count: int = 1,
    exclude_urls: set[str] | None = None,
    dry_run: bool = False,
    dry_label: str = "Image search",
) -> list[str]:
    """
    Search DuckDuckGo Images for up to `count` distinct accessible URLs
    from a single query attempt chain (original + fallbacks).
    """
    exclude = set(exclude_urls or set())
    fallback_words = _image_fallback_words(query)
    original = (query or "").strip()
    attempts: list[tuple[str, str]] = [("original", original)]
    original_lower = original.lower()
    for i, word in enumerate(fallback_words):
        if word.lower() == original_lower:
            continue
        attempts.append((f"fallback-{i + 1}", word))
    attempts = [(label, q) for label, q in attempts if q]
    attempts = attempts[:3]

    collected: list[str] = []
    for attempt_idx, (label, attempt_query) in enumerate(attempts, start=1):
        if len(collected) >= count:
            break
        need = count - len(collected)
        try:
            found = _ddg_search_accessible_images(
                attempt_query, exclude | set(collected), count=need
            )
            if found:
                log.info(
                    f"DDG image(s) found (accessible) on {label} "
                    f"attempt {attempt_idx}/{len(attempts)} "
                    f"query={attempt_query!r}: {len(found)} url(s)"
                )
                collected.extend(found)
            else:
                log.info(
                    f"DDG image search returned no accessible results on {label} "
                    f"attempt {attempt_idx}/{len(attempts)} query={attempt_query!r}"
                )
        except Exception as e:
            log.warning(
                f"DDG image search failed on {label} "
                f"attempt {attempt_idx}/{len(attempts)} "
                f"query={attempt_query!r}: {e}"
            )

    if dry_run:
        if collected:
            dry_print(
                dry_label,
                f"URLs ({len(collected)}): "
                + ", ".join(u[:60] for u in collected)
                + "\nAccessibility check: PASSED",
            )
        else:
            dry_print(
                dry_label,
                "URL: (none)\n"
                "Accessibility check: N/A — no accessible DDG result "
                f"after {len(attempts)} attempt(s)",
            )
    return collected[:count]


def resolve_featured_and_inline_images(
    image_query: str,
    feed_image: str | None,
    dry_run: bool = False,
) -> tuple[str | None, str | None]:
    """
    Resolve featured + inline image URLs without duplicate DDG work.

    Featured prefers an accessible RSS feed image, else DDG.
    When a feed image exists, its HEAD check overlaps a single DDG pass for
    up to two alternate URLs. When both slots need DDG, that one pass supplies both.
    """
    if feed_image:
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_acc = pool.submit(is_image_accessible, feed_image)
            fut_ddg = pool.submit(
                find_image_urls,
                image_query,
                count=2,
                exclude_urls={feed_image},
                dry_run=dry_run,
                dry_label="DDG images (alongside RSS check)",
            )
            feed_accessible = fut_acc.result()
            ddg_urls = fut_ddg.result()
        if dry_run:
            dry_print(
                "Featured image (from RSS)",
                f"URL: {feed_image}\n"
                f"Accessibility check: "
                f"{'PASSED' if feed_accessible else 'FAILED'}",
            )
        if feed_accessible:
            log.info(f"Using RSS feed image for featured: {feed_image[:80]}")
            inline = ddg_urls[0] if ddg_urls else None
            if inline:
                log.info(f"DDG inline image: {inline[:80]}")
            return feed_image, inline
        log.info("RSS feed image present but not accessible; using DDG pair")
        featured = ddg_urls[0] if ddg_urls else None
        inline = ddg_urls[1] if len(ddg_urls) > 1 else None
        if featured:
            log.info(f"DDG featured image: {featured[:80]}")
        if inline:
            log.info(f"DDG inline image: {inline[:80]}")
        return featured, inline

    # No feed image — one DDG pass for two distinct URLs
    urls = find_image_urls(
        image_query,
        count=2,
        dry_run=dry_run,
        dry_label="Featured + inline images (DDG)",
    )
    featured = urls[0] if urls else None
    inline = urls[1] if len(urls) > 1 else None
    if featured:
        log.info(f"DDG featured image: {featured[:80]}")
    if inline:
        log.info(f"DDG inline image: {inline[:80]}")
    return featured, inline


# ─── Gemini content generation ────────────────────────────────────────────────
def _is_model_not_found(exc: Exception) -> bool:
    """True when the error indicates the model ID is missing / unavailable (try next)."""
    msg = str(exc).lower()
    if "404" in msg or "not found" in msg or "is not found" in msg:
        return True
    # google.genai / google.api_core often surface NotFound with these codes
    code = getattr(exc, "code", None) or getattr(exc, "status_code", None)
    if code in (404, "404", "NOT_FOUND"):
        return True
    status = getattr(exc, "status", None)
    if status in (404, "NOT_FOUND"):
        return True
    return False


def _is_rate_limit_error(exc: Exception) -> bool:
    """True for Gemini/API 429 / RESOURCE_EXHAUSTED rate-limit signals."""
    code = getattr(exc, "code", None) or getattr(exc, "status_code", None)
    if code in (429, "429", "RESOURCE_EXHAUSTED"):
        return True
    status = getattr(exc, "status", None)
    if status in (429, "429", "RESOURCE_EXHAUSTED"):
        return True
    msg = str(exc).lower()
    return (
        "429" in msg
        or "resource_exhausted" in msg
        or "resource exhausted" in msg
        or "rate limit" in msg
        or "rate-limit" in msg
        or "too many requests" in msg
    )


def _retry_after_from_exception(exc: Exception) -> float | None:
    """Best-effort Retry-After extraction from SDK / HTTP exceptions."""
    candidates = [getattr(exc, "headers", None)]
    response = getattr(exc, "response", None)
    if response is not None:
        candidates.append(getattr(response, "headers", None))
    for headers in candidates:
        if not headers:
            continue
        try:
            value = headers.get("Retry-After") or headers.get("retry-after")
        except (AttributeError, TypeError):
            continue
        if value is None:
            continue
        parsed = _parse_retry_after_seconds(str(value))
        if parsed is not None:
            return parsed
    return None


def gemini_generate_content(client, *, model: str, contents, config, label: str):
    """
    client.models.generate_content with 429-aware retries (15/30/60 + jitter,
    Retry-After when present). Other errors propagate to the caller.
    """

    def _do():
        try:
            return client.models.generate_content(
                model=model,
                contents=contents,
                config=config,
            )
        except Exception as e:
            if _is_rate_limit_error(e):
                raise TransientHTTPError(
                    str(e),
                    status_code=429,
                    retry_after=_retry_after_from_exception(e),
                ) from e
            raise

    return call_with_http_retries(_do, label=label)


def _gemini_json_config(max_output_tokens: int) -> types.GenerateContentConfig:
    """JSON/text generation config with automatic function calling disabled."""
    return types.GenerateContentConfig(
        max_output_tokens=max_output_tokens,
        automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
    )


def _strip_json_fences(raw: str) -> str:
    """Strip markdown code fences if Gemini adds them despite instructions."""
    raw = re.sub(r"^```json\s*", "", raw)
    raw = re.sub(r"^```\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    return raw


def title_relevance_check(article: dict, dry_run: bool = False) -> bool | None:
    """
    Lightweight Gemini gate using only title + source name.
    Returns True if the article looks on-topic, False if off-topic (skip),
    or None if the check fails.
    """
    client = genai.Client(api_key=GEMINI_API_KEY)
    prompt = f"""
Your task: Decide whether this news article is a strong fit for the Xetarev Inlight blog
(an indie tech company blog covering software, AI, startups, developer tools, cybersecurity,
and indie building).
Use ONLY the title and source name — do not assume article body content.
Source article title: {article['title']}
Source: {article['source']}
Relevance criteria:
- Strong fit for: technology, software, AI, startups, developer tools, cybersecurity, or indie building.
- Not a fit: lifestyle fluff, unrelated politics, pure entertainment, education-for-teachers, generic vendor marketing, or other weak/tangential topics.
Respond ONLY with valid JSON, no markdown fences, no extra text:
- If it is a strong fit: {{"relevant": true}}
- If it is not a strong fit: {{"skip": true}}
"""
    last_error = None
    for model_name in MODEL_CHAIN:
        try:
            response = gemini_generate_content(
                client,
                model=model_name,
                contents=prompt,
                config=_gemini_json_config(32),
                label=f"Gemini title check ({model_name})",
            )
            raw = _strip_json_fences(response.text.strip())
            if dry_run:
                dry_print(
                    "Title-first relevance check (raw Gemini response)",
                    raw,
                )
            data = json.loads(raw)
            if data.get("skip"):
                log.info(
                    f"Gemini ({model_name}) title-skip (off-topic): "
                    f"{article['title'][:60]}"
                )
                return False
            if data.get("relevant"):
                log.info(
                    f"Gemini ({model_name}) title-pass (on-topic): "
                    f"{article['title'][:60]}"
                )
                return True
            log.warning(
                f"Gemini ({model_name}) title check returned unexpected JSON: {raw[:200]}"
            )
            return None
        except json.JSONDecodeError as e:
            log.error(
                f"Gemini ({model_name}) title check invalid JSON: {e}\n"
                f"Raw response: {raw[:300]}"
            )
            return None
        except TransientHTTPError as e:
            last_error = e
            log.error(f"Gemini ({model_name}) rate-limited after retries: {e}")
            return None
        except Exception as e:
            last_error = e
            if _is_model_not_found(e):
                log.warning(f"Model {model_name} not found (404), trying next: {e}")
                continue
            log.error(f"Gemini title check failed with {model_name}: {e}")
            return None
    log.error(f"All models in MODEL_CHAIN failed title check. Last error: {last_error}")
    return None


def generate_post(article: dict, body_text: str, dry_run: bool = False) -> dict | None:
    """
    Call Gemini to produce a structured JSON response with all post fields.
    Tries each model in MODEL_CHAIN until one succeeds.
    Returns a dict with: title, slug, excerpt, paragraphs (list of strings), keywords (list)
    Returns None if generation fails.
    Assumes the article already passed title_relevance_check.
    """
    client = genai.Client(api_key=GEMINI_API_KEY)
    prompt = f"""
{BRAND_CONTEXT}
Your task: Write a blog post for the Xetarev Inlight blog based on the news article below.
Source article title: {article['title']}
Source: {article['source']}
Article text:
\"\"\"
{body_text[:3000]}
\"\"\"
Instructions:
- Write a fresh, original post. Do NOT copy sentences from the source. Rewrite everything.
- Length: 500-550 words across 4–6 paragraphs.
- Voice: knowledgeable individual with a point of view. First person where natural. Direct opinions. Sharp, informed, human — not an AI content pipeline.
- Burstiness: deliberately mix short sentences (under ~8 words) with longer ones (30+ words). Avoid uniform sentence length.
- End the post naturally. Do NOT mention Xetarev, Xetarev Studio, or Xetarev Apps.
- SEO-optimized: use the main topic keyword naturally in the title and early in the body.
- The excerpt should be 1–2 sentences, punchy, suitable for a feed preview.
- Paragraph 3 or 4 should include [IMAGE] as a placeholder on its own line — this is where the inline image will be inserted.
Respond ONLY with valid JSON, no markdown fences, no extra text. Format:
{{
  "title": "...",
  "slug": "...",
  "excerpt": "...",
  "paragraphs": [
    "First paragraph text.",
    "Second paragraph text.",
    "[IMAGE]",
    "Fourth paragraph text.",
    "Fifth paragraph text."
  ],
  "keywords": ["keyword1", "keyword2", "keyword3"]
}}
"""
    last_error = None
    for model_name in MODEL_CHAIN:
        try:
            response = gemini_generate_content(
                client,
                model=model_name,
                contents=prompt,
                config=_gemini_json_config(1000),
                label=f"Gemini generate_post ({model_name})",
            )
            raw = response.text.strip()
            if dry_run:
                dry_print(
                    "Raw Gemini response (before JSON parsing)",
                    f"chars: {len(raw)} (body omitted)",
                )
            data = json.loads(_strip_json_fences(raw))
            log.info(f"Gemini ({model_name}) generated post: {data.get('title', '')[:60]}")
            if dry_run:
                paragraphs = data.get("paragraphs") or []
                para_summary = "\n".join(
                    f"  [{i}] {len(p)} chars"
                    + (" [IMAGE]" if p.strip() == IMAGE_PLACEHOLDER else "")
                    for i, p in enumerate(paragraphs, start=1)
                )
                dry_print(
                    "Parsed post fields",
                    f"title: {data.get('title', '')}\n"
                    f"slug: {data.get('slug', '')}\n"
                    f"excerpt: {data.get('excerpt', '')}\n"
                    f"keywords: {data.get('keywords', [])}\n"
                    f"paragraphs ({len(paragraphs)}) — body omitted:\n{para_summary}",
                )
            return data
        except json.JSONDecodeError as e:
            log.error(f"Gemini ({model_name}) returned invalid JSON: {e}\nRaw response: {raw[:300]}")
            return None
        except TransientHTTPError as e:
            last_error = e
            log.error(f"Gemini ({model_name}) rate-limited after retries: {e}")
            return None
        except Exception as e:
            last_error = e
            if _is_model_not_found(e):
                log.warning(f"Model {model_name} not found (404), trying next: {e}")
                continue
            log.error(f"Gemini call failed with {model_name}: {e}")
            return None
    log.error(f"All models in MODEL_CHAIN failed. Last error: {last_error}")
    return None


# ─── Humanization + AI-audit pipeline ─────────────────────────────────────────
# Note: "humanizer-skill" is not published on PyPI (Markdown/Node agent skill).
# Stage 3 applies that skill's pattern catalog as a local deterministic pass.
IMAGE_PLACEHOLDER = "[IMAGE]"

# Keys are lemma/base forms. Matching covers common inflections and, for
# hyphenated keys, hyphen/space/dash variants (see _ai_vocab_pattern).
# Kept in sync with ai_text_audit ai_vocabulary + promotional catalogs.
_AI_VOCAB_REPLACEMENTS: dict[str, str] = {
    # ai_vocabulary (auditor)
    "delve": "look into",
    "crucial": "important",
    "landscape": "field",
    "leverage": "use",
    "multifaceted": "complex",
    "comprehensive": "thorough",
    "underscore": "point out",
    "foster": "build",
    "tapestry": "mix",
    "pivotal": "key",
    "nuance": "subtle",
    "robust": "strong",
    "paradigm": "model",
    "synergy": "teamwork",
    "holistic": "overall",
    "testament": "proof",
    "vibrant": "lively",
    "intricate": "detailed",
    "poignant": "sharp",
    "seamless": "smooth",
    "garner": "get",
    "emphasize": "stress",
    "highlight": "point out",
    # promotional buzzwords (auditor) — confirmed slips: unprecedented,
    # state-of-the-art, intricate (+ rest of promotional catalog)
    "unprecedented": "rare",
    "unparalleled": "rare",
    "groundbreaking": "new",
    "cutting-edge": "latest",
    "state-of-the-art": "latest",
    "game-changer": "big change",
    "revolutionary": "major",
    "breathtaking": "striking",
    "stunning": "striking",
    "transformative": "major",
    "disruptive": "major",
    "innovative solution": "new approach",
    "innovative approach": "new approach",
    # extra common AI filler still worth scrubbing
    "facilitate": "help",
    "streamline": "simplify",
    "harness": "use",
    "illuminate": "show",
    "embark": "start",
    "endeavor": "effort",
    "showcase": "show",
    "bolster": "strengthen",
    "myriad": "many",
    "plethora": "plenty",
}

_AI_PHRASE_REMOVALS: tuple[str, ...] = (
    "it's important to note that",
    "it is important to note that",
    "it's worth noting that",
    "it is worth noting that",
    "it's important to note",
    "it is important to note",
    "it's worth noting",
    "it is worth noting",
    "in today's rapidly evolving",
    "in today's fast-paced",
    "in the ever-evolving",
    "in the realm of",
    "at its core",
    "at the end of the day",
    "when it comes to",
    "as we move forward",
    "going forward",
    "looking ahead",
    "without further ado",
    "let's dive in",
    "let us dive in",
    "in conclusion",
    "to summarize",
    "needless to say",
)

_HEDGING_PHRASES: tuple[str, ...] = (
    "could potentially",
    "might possibly",
    "it could be argued that",
    "it might be argued that",
    "it seems that",
    "it appears that",
    "in some ways",
    "to some extent",
    "generally speaking",
)

_DELETE_WORDS: tuple[str, ...] = (
    "moreover",
    "furthermore",
    "arguably",
)


def _join_paragraphs(paragraphs: list[str]) -> str:
    return "\n\n".join(paragraphs)


def _new_isolated_auditor():
    """
    Build an Auditor whose Pattern objects are deep-copied off the library
    registry.

    ai_text_audit.get_patterns() only shallow-copies the registry list, so
    every Auditor() would otherwise share the same Pattern instances (and
    their cached _compiled regexes) process-wide. Isolation keeps each
    audit self-contained.
    """
    from ai_text_audit import Auditor

    auditor = Auditor(language="all")
    auditor.patterns = [copy.deepcopy(p) for p in auditor.patterns]
    for pattern in auditor.patterns:
        pattern._compiled = None
    return auditor


# Process-wide auditor: created once, patterns isolated from the package registry.
# Auditor.analyze() itself does not accumulate per-call score state; reuse is safe.
_AUDITOR = None


def _get_auditor():
    global _AUDITOR
    if _AUDITOR is None:
        _AUDITOR = _new_isolated_auditor()
    return _AUDITOR


def _audit_text(
    text: str, *, dry_run: bool = False, label: str = "AI Audit"
) -> float | None:
    """
    Run ai_text_audit on text via the process-wide isolated Auditor.
    Returns score, or None on failure. Empty text is logged and returns 0.0
    only after being flagged — callers should not treat it as a clean pass.
    """
    try:
        from ai_text_audit.cli import format_terminal

        auditor = _get_auditor()
        result = auditor.analyze(text)

        # Empty input always scores 0 in the library — do not treat as "clean".
        if result.details.get("error") == "Empty text" or result.word_count == 0:
            log.warning(
                f"{label}: empty or whitespace-only text "
                f"(score={result.score}, words={result.word_count})"
            )
            if dry_run:
                dry_print(
                    label,
                    format_terminal(result, filepath="generated post", verbose=True)
                    + "\n\nNOTE: empty text — score 0 is not a clean human-written pass.",
                )
            return float(result.score)

        # Zero on real content: recheck with a brand-new isolated instance so a
        # shared/cached Pattern bug cannot silently zero one article per run.
        if result.score == 0.0:
            verify = _new_isolated_auditor().analyze(text)
            if verify.score != result.score:
                log.error(
                    f"{label}: auditor recheck mismatch "
                    f"(singleton={result.score} fresh={verify.score}); "
                    f"using fresh result"
                )
                result = verify
            else:
                log.info(
                    f"{label}: score 0.0 confirmed on fresh recheck "
                    f"({result.word_count} words, "
                    f"{result.details.get('patterns_checked')} patterns checked, "
                    f"no AI-writing regex hits)"
                )

        score = float(result.score)
        log.info(
            f"{label}: score={score:.1f} verdict={result.verdict} "
            f"words={result.word_count} patterns={len(result.patterns)} "
            f"checked={result.details.get('patterns_checked')}"
        )
        for p in result.patterns:
            log.info(f"  pattern={p.name} severity={p.severity} count={p.count}")
        if dry_run:
            preview = text[:240].replace("\n", " ")
            if len(text) > 240:
                preview += "…"
            dry_print(
                label,
                format_terminal(result, filepath="generated post", verbose=True)
                + f"\n\nAudited text preview ({result.word_count} words):\n{preview}",
            )
        return score
    except Exception as e:
        log.error(f"{label} failed (skipped): {e}")
        return None


def _map_paragraphs(paragraphs: list[str], transform) -> list[str]:
    """Apply transform to each paragraph; keep [IMAGE] exactly as-is."""
    out: list[str] = []
    for para in paragraphs:
        if para.strip() == IMAGE_PLACEHOLDER:
            out.append(IMAGE_PLACEHOLDER)
        else:
            out.append(transform(para))
    return out


def _case_aware_replace(matched: str, replacement: str) -> str:
    if not replacement:
        return ""
    if matched.isupper():
        return replacement.upper()
    if matched[0].isupper():
        return replacement[0].upper() + replacement[1:]
    return replacement


def _ai_vocab_pattern(word: str) -> re.Pattern:
    """
    Match a vocab lemma plus common English inflections.
    Exact \\bword\\b misses pairs like nuance↔nuanced (silent-e + d).
    Hyphenated / multi-word keys also match space and dash variants
    (cutting-edge ↔ cutting edge; state-of-the-art ↔ state of the art),
    mirroring ai_text_audit's promotional '.' wildcards.
    """
    parts = re.split(r"[-–—\s]+", word.strip())
    parts = [p for p in parts if p]
    if len(parts) > 1:
        # Compound / phrase: flexible separators, optional trailing plural on last part
        body = r"[-–—\s]+".join(re.escape(p) for p in parts)
        last = parts[-1]
        if last.endswith("e"):
            plural = r"s?"
        else:
            plural = r"(?:s|es)?"
        pat = rf"\b{body}{plural}\b"
        return re.compile(pat, re.IGNORECASE)

    escaped = re.escape(word)
    if word.endswith("e"):
        stem = re.escape(word[:-1])
        # nuance → nuances / nuanced / nuancing; comprehensive → comprehensively
        pat = rf"\b(?:{escaped}(?:s|d|ly)?|{stem}ing)\b"
    else:
        pat = rf"\b{escaped}(?:s|es|ed|ing|ly)?\b"
    return re.compile(pat, re.IGNORECASE)


def _contains_replaceable_ai_vocab(text: str) -> bool:
    """True if any lemma in _AI_VOCAB_REPLACEMENTS (incl. inflections) remains."""
    return any(_ai_vocab_pattern(word).search(text) for word in _AI_VOCAB_REPLACEMENTS)


def _humanizer_skill_pattern_pass(text: str) -> str:
    """
    Deterministic cleanup pass mirroring the humanizer-skill pattern catalog:
    hollow openers, hedging, AI vocabulary, and leftover filler.
    Em dashes are preserved (burstiness prompts inject them intentionally).
    """
    rewrite = text

    for phrase in _AI_PHRASE_REMOVALS:
        pattern = re.compile(re.escape(phrase) + r"[\s,]*", re.IGNORECASE)
        rewrite = pattern.sub("", rewrite)

    for phrase in _HEDGING_PHRASES:
        pattern = re.compile(re.escape(phrase) + r"[\s,]*", re.IGNORECASE)
        rewrite = pattern.sub("", rewrite)

    for word in _DELETE_WORDS:
        pattern = re.compile(r"\b" + re.escape(word) + r"\b[\s,]*", re.IGNORECASE)
        rewrite = pattern.sub("", rewrite)

    # Longer lemmas first so e.g. "cutting-edge" wins over a hypothetical shorter key.
    for word, replacement in sorted(
        _AI_VOCAB_REPLACEMENTS.items(), key=lambda item: len(item[0]), reverse=True
    ):
        pattern = _ai_vocab_pattern(word)

        def _swap(match: re.Match, _repl: str = replacement) -> str:
            return _case_aware_replace(match.group(0), _repl)

        rewrite = pattern.sub(_swap, rewrite)

    # Keep intentional em dashes / en dashes — burstiness prompts use them on purpose.

    rewrite = re.sub(r" {2,}", " ", rewrite)
    rewrite = re.sub(r" ([,.!?;:])", r"\1", rewrite)
    rewrite = re.sub(r"\n{3,}", "\n\n", rewrite)
    return rewrite.strip()


def _apply_humanize_stages(
    paragraphs: list[str], dry_run: bool = False, label_prefix: str = ""
) -> list[str]:
    """Run Stage 2 (TextHumanize) then Stage 3 (pattern pass)."""
    paras = list(paragraphs or [])
    prefix = f"{label_prefix} — " if label_prefix else ""

    # Stage 2 — TextHumanize
    try:
        def _th(para: str) -> str:
            return humanize(para, lang="en").text

        paras = _map_paragraphs(paras, _th)
        log.info(f"{prefix}TextHumanize ran")
        if dry_run:
            dry_print(
                f"{prefix}Stage 2 — TextHumanize paragraph text (after)",
                f"paragraphs: {len(paras)}, chars: {len(_join_paragraphs(paras))} (body omitted)",
            )
    except Exception as e:
        log.error(f"{prefix}Stage 2 TextHumanize failed (skipped): {e}")

    # Stage 3 — Humanizer-Skill pattern pass
    try:
        scrub_passes = 0
        max_scrub_passes = 2
        while scrub_passes < max_scrub_passes:
            paras = _map_paragraphs(paras, _humanizer_skill_pattern_pass)
            scrub_passes += 1
            if not _contains_replaceable_ai_vocab(_join_paragraphs(paras)):
                break
        if _contains_replaceable_ai_vocab(_join_paragraphs(paras)):
            paras = _map_paragraphs(paras, _humanizer_skill_pattern_pass)
            scrub_passes += 1
            log.info(f"{prefix}Residual AI-vocab pattern scrub ran")
        log.info(
            f"{prefix}Humanizer-Skill pattern pass ran ({scrub_passes} scrub pass(es))"
        )
        if dry_run:
            dry_print(
                f"{prefix}Stage 3 — Humanizer-Skill pattern pass paragraph text (after)",
                f"paragraphs: {len(paras)}, chars: {len(_join_paragraphs(paras))} "
                f"(body omitted); scrub_passes={scrub_passes}",
            )
    except Exception as e:
        log.error(f"{prefix}Stage 3 Humanizer-Skill pattern pass failed (skipped): {e}")

    return paras


def _run_stage4_audit(
    paragraphs: list[str], dry_run: bool = False, label: str = "Stage 4 — AI Audit"
) -> float | None:
    """Always run AI audit; return score or None on failure."""
    text = _join_paragraphs(paragraphs)
    score = _audit_text(text, dry_run=dry_run, label=label)
    if dry_run and score is not None:
        dry_print(
            f"{label} — paragraph text (after)",
            f"paragraphs: {len(paragraphs)}, chars: {len(text)} (body omitted)",
        )
    return score


def rewrite_paragraphs_for_human_voice(
    paragraphs: list[str],
    *,
    dry_run: bool = False,
    retry_num: int = 1,
) -> list[str] | None:
    """
    Ask Gemini to rewrite only the body paragraphs to sound more human.
    Returns a new paragraphs list, or None on failure.
    """
    client = genai.Client(api_key=GEMINI_API_KEY)
    body = _join_paragraphs(paragraphs)
    prompt = f"""
{BRAND_CONTEXT}
Your task: Rewrite the blog post body below so it sounds more human and less AI-generated.
Prioritize burstiness at the sentence and paragraph level — not vague "more conversational" tone.

Current body paragraphs:
\"\"\"
{body[:4000]}
\"\"\"

Structural requirements (non-negotiable):
- Every paragraph must contain at least one sentence under 8 words
  AND one sentence over 30 words.
- Prefer even sharper contrast where natural: at least one sentence over 35 words
  and one under 4 words somewhere in the post (fragments OK).
- No two consecutive sentences may start with the same word.
- Include one rhetorical question somewhere in the post.
- Use at least one em-dash interruption or parenthetical aside.
- One paragraph may start with "But" or "And" — this is intentional.
- Vary paragraph length: at least one paragraph under 40 words,
  one over 80 words.

Also:
- Cut AI vocabulary, hedging, and promotional buzzwords.
- Keep the same ideas and approximate length (350–500 words across 4–6 paragraphs).
- If a paragraph is exactly [IMAGE], keep that line as its own paragraph unchanged.
- Do NOT change or invent a title, slug, excerpt, or keywords — rewrite paragraphs only.

Respond ONLY with valid JSON, no markdown fences, no extra text:
{{
  "paragraphs": [
    "First rewritten paragraph.",
    "Second rewritten paragraph.",
    "[IMAGE]",
    "…"
  ]
}}
"""
    label = f"Retry {retry_num} — Gemini human rewrite"
    last_error = None
    for model_name in MODEL_CHAIN:
        try:
            response = gemini_generate_content(
                client,
                model=model_name,
                contents=prompt,
                config=_gemini_json_config(1000),
                label=f"{label} ({model_name})",
            )
            raw = response.text.strip()
            if dry_run:
                dry_print(
                    f"{label} — raw response (before JSON parsing)",
                    f"model: {model_name}\nchars: {len(raw)} (body omitted)",
                )
            raw = re.sub(r"^```json\s*", "", raw)
            raw = re.sub(r"^```\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            data = json.loads(raw)
            new_paras = data.get("paragraphs") or []
            has_paragraph = any(
                isinstance(p, str) and p.strip() and p.strip() != IMAGE_PLACEHOLDER
                for p in new_paras
            )
            if not has_paragraph:
                log.warning(f"{label} ({model_name}): rewrite missing usable paragraphs")
                return None
            log.info(f"{label} ({model_name}): got {len(new_paras)} paragraphs")
            if dry_run:
                para_summary = "\n".join(
                    f"  [{i}] {len(p)} chars"
                    + (" [IMAGE]" if str(p).strip() == IMAGE_PLACEHOLDER else "")
                    for i, p in enumerate(new_paras, start=1)
                )
                dry_print(
                    f"{label} — rewritten paragraphs",
                    f"paragraphs ({len(new_paras)}) — body omitted:\n{para_summary}",
                )
            return [str(p) for p in new_paras]
        except json.JSONDecodeError as e:
            log.error(f"{label} ({model_name}) invalid JSON: {e}\nRaw: {raw[:300]}")
            return None
        except TransientHTTPError as e:
            last_error = e
            log.error(f"{label} ({model_name}) rate-limited after retries: {e}")
            return None
        except Exception as e:
            last_error = e
            if _is_model_not_found(e):
                log.warning(f"Model {model_name} not found (404), trying next: {e}")
                continue
            log.error(f"{label} failed with {model_name}: {e}")
            return None
    log.error(f"{label}: all models failed. Last error: {last_error}")
    return None


def humanize_and_audit_paragraphs(
    paragraphs: list[str], dry_run: bool = False
) -> list[str]:
    """
    Humanize generated paragraphs, then gate on Stage 4 AI-audit score.
      1. AI audit (baseline) — dry-run only
      2. TextHumanize
      3. Humanizer-skill pattern catalog pass
      4. AI audit (always) — if score >= HUMAN_SCORE_THRESHOLD, Gemini rewrite
         up to MAX_HUMANIZE_RETRIES times (re-run stages 2–3 + audit each time).
         After Retry 2, proceed with the latest draft regardless of score.
    Only paragraphs are rewritten on retries; title/slug/excerpt/keywords stay put.
    """
    paras = list(paragraphs or [])
    baseline_score: float | None = None

    # Stage 1 — AI Audit (baseline, dry-run only)
    if dry_run:
        baseline_score = _audit_text(
            _join_paragraphs(paras),
            dry_run=True,
            label="Stage 1 — AI Audit (baseline)",
        )
        dry_print(
            "Stage 1 — paragraph text (after)",
            f"paragraphs: {len(paras)}, chars: {len(_join_paragraphs(paras))} (body omitted)",
        )

    # Stages 2–3 (initial)
    paras = _apply_humanize_stages(paras, dry_run=dry_run)

    # Stage 4 — AI Audit (always); rewrite retries if score is too high
    score = _run_stage4_audit(
        paras, dry_run=dry_run, label="Stage 4 — AI Audit (post-humanization)"
    )
    retries_used = 0

    while (
        score is not None
        and score >= HUMAN_SCORE_THRESHOLD
        and retries_used < MAX_HUMANIZE_RETRIES
    ):
        retries_used += 1
        log.info(
            f"Stage 4 score {score:.1f} >= {HUMAN_SCORE_THRESHOLD} "
            f"— starting Retry {retries_used}/{MAX_HUMANIZE_RETRIES}"
        )
        if dry_run:
            dry_print(
                f"Retry {retries_used} — starting",
                f"Score {score:.1f} is at or above Likely-Human threshold "
                f"({HUMAN_SCORE_THRESHOLD}). Sending paragraphs to Gemini for "
                f"a more human rewrite. Title/slug/excerpt/keywords unchanged.",
            )

        # Pause before another Gemini call (same courtesy as between articles)
        time.sleep(3)
        rewritten = rewrite_paragraphs_for_human_voice(
            paras, dry_run=dry_run, retry_num=retries_used
        )
        if not rewritten:
            log.warning(
                f"Retry {retries_used} rewrite failed; keeping current paragraphs"
            )
            if dry_run:
                dry_print(
                    f"Retry {retries_used} — rewrite failed",
                    "Keeping current paragraphs; will continue retry loop or proceed.",
                )
            continue

        paras = _apply_humanize_stages(
            rewritten, dry_run=dry_run, label_prefix=f"Retry {retries_used}"
        )
        score = _run_stage4_audit(
            paras,
            dry_run=dry_run,
            label=f"Retry {retries_used} — Stage 4 AI Audit",
        )

    if score is not None and score >= HUMAN_SCORE_THRESHOLD:
        log.info(
            f"Proceeding after {retries_used} rewrite retry(ies) with score "
            f"{score:.1f} (threshold {HUMAN_SCORE_THRESHOLD})"
        )
    elif score is not None:
        log.info(
            f"Stage 4 cleared threshold: score={score:.1f} "
            f"(retries used: {retries_used})"
        )
    else:
        log.info(f"Stage 4 audit unavailable; proceeding (retries used: {retries_used})")

    if dry_run and baseline_score is not None and score is not None:
        delta = score - baseline_score
        dry_print(
            "AI-tell score comparison (baseline → final)",
            f"AI-tell score before: {baseline_score:.1f}\n"
            f"AI-tell score after:  {score:.1f}\n"
            f"Delta:                {delta:+.1f}\n"
            f"Retries used:         {retries_used}",
        )

    return paras


# ─── Lexical JSON builder ─────────────────────────────────────────────────────
def build_lexical_content(
    paragraphs: list[str], image_url: str | None, image_alt: str
) -> dict:
    """
    Build a Payload Lexical content JSON object from a list of paragraph strings.
    Matches exactly the structure observed in your existing inlight_posts rows:
    - paragraph nodes for text
    - imageBlock custom block nodes for images (using externalUrl)
    Any paragraph that is exactly "[IMAGE]" becomes an imageBlock node when
    image_url is set; otherwise the placeholder is omitted.
    """
    children = []
    # Leading empty paragraph (matches your existing post structure)
    children.append({
        "type": "paragraph",
        "format": "",
        "indent": 0,
        "version": 1,
        "children": [{
            "mode": "normal",
            "text": "",
            "type": "text",
            "style": "",
            "detail": 0,
            "format": 0,
            "version": 1
        }],
        "direction": "ltr"
    })
    image_inserted = False
    for para in paragraphs:
        if para.strip() == "[IMAGE]":
            if not image_url:
                continue  # no accessible image — skip placeholder, no node
            # Insert the inline imageBlock — same structure as your existing posts
            block_id = hashlib.md5(f"{image_url}{time.time()}".encode()).hexdigest()[:36]
            children.append({
                "type": "block",
                "fields": {
                    "id": block_id,
                    "alt": image_alt,
                    "caption": "",
                    "blockName": "",
                    "blockType": "imageBlock",
                    "externalUrl": image_url
                },
                "format": "",
                "version": 2
            })
            image_inserted = True
        else:
            children.append({
                "type": "paragraph",
                "format": "",
                "indent": 0,
                "version": 1,
                "children": [{
                    "mode": "normal",
                    "text": para.strip(),
                    "type": "text",
                    "style": "",
                    "detail": 0,
                    "format": 0,
                    "version": 1
                }],
                "direction": "ltr"
            })
    # If Gemini didn't include [IMAGE] in the output, append image after second paragraph
    if not image_inserted and image_url:
        block_id = hashlib.md5(f"{image_url}{time.time()}".encode()).hexdigest()[:36]
        image_node = {
            "type": "block",
            "fields": {
                "id": block_id,
                "alt": image_alt,
                "caption": "",
                "blockName": "",
                "blockType": "imageBlock",
                "externalUrl": image_url
            },
            "format": "",
            "version": 2
        }
        # Insert after the second real paragraph (index 2, after the leading empty + first para)
        insert_at = min(3, len(children))
        children.insert(insert_at, image_node)
    return {
        "root": {
            "type": "root",
            "format": "",
            "indent": 0,
            "version": 1,
            "children": children,
            "direction": "ltr"
        }
    }
# ─── Payload REST API POST ────────────────────────────────────────────────────
def get_payload_jwt() -> str | None:
    """Log in to Payload and return a short-lived JWT (retries transient failures)."""
    login_url = PAYLOAD_API_URL.replace("/api/inlight-posts", "/api/users/login")
    try:
        r = http_request(
            "POST",
            login_url,
            label="Payload login",
            json={
                "email": os.environ["PAYLOAD_EMAIL"],
                "password": os.environ["PAYLOAD_PASSWORD"],
            },
            timeout=10,
        )
        if r.status_code == 200:
            token = r.json().get("token")
            log.info("Payload JWT obtained")
            return token
        log.error(f"Payload login failed {r.status_code}: {r.text[:200]}")
        return None
    except TransientHTTPError as e:
        log.error(f"Payload login failed after retries: {e}")
        return None
    except Exception as e:
        log.error(f"Payload login error: {e}")
        return None


def post_to_payload(
    generated: dict, content_json: dict, featured_image_url: str | None, jwt: str
) -> bool:
    """
    POST the generated post to Payload CMS as a published document.
    Field names match exactly what's in your inlight_posts table.
    Omits featuredImage when featured_image_url is None.
    Returns True on success, False on permanent failure (e.g. duplicate slug, 4xx).
    Raises TransientHTTPError when retries are exhausted on timeouts / 5xx / network errors
    so the caller can leave the article unseen for a later run.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    # Ensure slug is URL-safe
    safe_slug = slugify(generated.get("slug") or generated.get("title", "post"))
    # Check if slug already exists
    check = http_request(
        "GET",
        PAYLOAD_API_URL,
        label=f"Payload slug check ({safe_slug})",
        max_attempts=PAYLOAD_HTTP_MAX_ATTEMPTS,
        params={"where[slug][equals]": safe_slug, "limit": 1},
        headers={"Authorization": f"JWT {jwt}"},
        timeout=10,
    )
    if check.status_code == 200 and check.json().get("totalDocs", 0) > 0:
        log.warning(f"Slug already exists, skipping: {safe_slug}")
        return False
    payload = {
        "title":         generated["title"],
        "slug":          safe_slug,
        "excerpt":       generated.get("excerpt", ""),
        "content":       content_json,
        "meta": {
            "title":       generated["title"],
            "description": generated.get("excerpt", ""),
        },
        "categories": [5],
        "publishedAt":   now_iso,
        "_status":       "published",
    }
    if featured_image_url:
        payload["featuredImage"] = {
            "url": featured_image_url,
            "alt": generated["title"],
        }
    headers = {
        "Authorization": f"JWT {jwt}",
        "Content-Type":  "application/json",
    }
    r = http_request(
        "POST",
        PAYLOAD_API_URL,
        label=f"Payload POST ({safe_slug})",
        max_attempts=PAYLOAD_HTTP_MAX_ATTEMPTS,
        json=payload,
        headers=headers,
        timeout=15,
    )
    if r.status_code in (200, 201):
        doc = r.json()
        doc_id = doc.get("doc", {}).get("id") or doc.get("id", "unknown")
        log.info(f"✅ Published post ID {doc_id}: {generated['title'][:60]}")
        return True
    log.error(f"Payload API returned {r.status_code}: {r.text[:300]}")
    return False
# ─── Main pipeline ────────────────────────────────────────────────────────────
def main(dry_run: bool = False):
    log.info("=== Xetarev Crawlspace (Agent) initializing ===")
    if dry_run:
        dry_print(
            "Mode",
            "Dry-run enabled — Payload POST, seen_articles.json, and "
            "feed_quality.json updates are skipped.\n"
            "Pipeline still fetches feeds, runs the title-first relevance gate, "
            "extracts text, calls Gemini, and resolves images.",
        )

    jwt = None
    if not dry_run:
        jwt = get_payload_jwt()
        if not jwt:
            log.error("Could not obtain Payload JWT. Exiting.")
            return

    seen = load_seen()
    feed_stats = load_feed_stats()
    log.info(f"Loaded {len(seen)} previously seen article IDs")
    log.info(f"Loaded quality stats for {len(feed_stats)} feed(s)")

    published_count = 0
    processed_count = 0
    used_feeds: set[str] = set()

    while published_count < MIN_PUBLISH:
        articles = fetch_new_articles(
            seen, used_feeds, feed_stats=feed_stats, dry_run=dry_run
        )
        if not articles:
            # Empty batch can mean either: selected feeds had only seen articles,
            # or every feed has already been polled. Keep going while unused feeds remain.
            unused = sum(1 for _, url in RSS_FEEDS if url not in used_feeds)
            if unused <= 0:
                log.info("No more unseen articles available. Stopping.")
                break
            log.info(
                f"No unseen articles in this batch; {unused} unused feed(s) remain — continuing"
            )
            continue

        log.info(
            f"Batch start — need {MIN_PUBLISH - published_count} more publish(es) "
            f"(published so far: {published_count}/{MIN_PUBLISH})"
        )

        for article in articles:
            if published_count >= MIN_PUBLISH:
                break

            log.info(f"--- Processing: {article['title'][:70]} ---")
            processed_count += 1

            if dry_run:
                dry_print(
                    "Raw RSS article",
                    f"title:  {article.get('title', '')}\n"
                    f"url:    {article.get('url', '')}\n"
                    f"source: {article.get('source', '')}",
                )

            # 1. Title-first relevance gate — before any article-body HTTP fetch
            relevant = title_relevance_check(article, dry_run=dry_run)
            if relevant is False:
                log.info(
                    f"Skipping off-topic article (title gate, marked seen): "
                    f"{article['title'][:70]}"
                )
                if dry_run:
                    dry_print(
                        "Outcome",
                        "SKIPPED — title-first relevance gate (skip: true); "
                        "no article body fetch",
                    )
                else:
                    record_feed_outcome(
                        feed_stats,
                        article.get("feed_url", ""),
                        article.get("source", ""),
                        "skipped",
                    )
                seen.add(article["id"])
                continue
            if relevant is None:
                log.warning("Title relevance check failed, skipping.")
                if dry_run:
                    dry_print(
                        "Outcome",
                        "SKIPPED — title-first relevance check failed; "
                        "no article body fetch",
                    )
                seen.add(article["id"])
                continue

            # 2. Get article text (may HTTP-fetch via trafilatura)
            try:
                body_text = get_article_text(article, dry_run=dry_run)
            except TransientHTTPError as e:
                log.warning(
                    f"Article body fetch failed after retries — leaving unseen: {e}"
                )
                if dry_run:
                    dry_print(
                        "Outcome",
                        "DEFERRED — article body HTTP fetch failed after retries; "
                        "not marked seen",
                    )
                continue
            if not body_text:
                log.warning("No usable text, skipping.")
                if dry_run:
                    dry_print(
                        "Outcome",
                        "SKIPPED — no usable article text extracted",
                    )
                seen.add(article["id"])
                continue

            # 3. Generate post content via Gemini
            generated = generate_post(article, body_text, dry_run=dry_run)
            if not generated:
                log.warning("Gemini generation failed, skipping.")
                if dry_run:
                    dry_print(
                        "Outcome",
                        "SKIPPED — Gemini generation failed or returned invalid JSON",
                    )
                seen.add(article["id"])
                continue

            title = (generated.get("title") or "").strip()
            paragraphs = generated.get("paragraphs") or []
            has_paragraph = any(
                isinstance(p, str) and p.strip() and p.strip() != IMAGE_PLACEHOLDER
                for p in paragraphs
            )
            if not title or not has_paragraph:
                log.warning(
                    "Gemini response missing title or paragraphs, skipping: "
                    f"title={bool(title)} paragraphs={has_paragraph}"
                )
                if dry_run:
                    dry_print(
                        "Outcome",
                        "SKIPPED — Gemini response missing non-empty title or paragraphs",
                    )
                seen.add(article["id"])
                continue

            # Content decision: Gemini approved this article — credit the feed now.
            # Payload/CMS outcomes must never touch feed_quality stats.
            if not dry_run:
                record_feed_outcome(
                    feed_stats,
                    article.get("feed_url", ""),
                    article.get("source", ""),
                    "published",
                )

            # 4. Featured/inline images + humanize/audit in parallel (independent work)
            image_query = " ".join(generated.get("keywords", [])[:3]) or article["title"][:40]
            with ThreadPoolExecutor(max_workers=2) as pool:
                fut_imgs = pool.submit(
                    resolve_featured_and_inline_images,
                    image_query,
                    article.get("image"),
                    dry_run,
                )
                fut_human = pool.submit(
                    humanize_and_audit_paragraphs,
                    generated.get("paragraphs", []),
                    dry_run,
                )
                featured_image_url, inline_image_url = fut_imgs.result()
                generated["paragraphs"] = fut_human.result()

            # 5. Build Lexical content JSON
            content_json = build_lexical_content(
                paragraphs=generated.get("paragraphs", []),
                image_url=inline_image_url,
                image_alt=generated["title"],
            )

            if dry_run:
                paras = generated.get("paragraphs") or []
                dry_print(
                    "Final Lexical JSON (would be sent to Payload)",
                    f"root children: {len((content_json.get('root') or {}).get('children') or [])}\n"
                    f"paragraphs: {len(paras)}, total chars: "
                    f"{sum(len(p) for p in paras)} (body omitted)",
                )
                dry_print(
                    "Outcome",
                    "WOULD PUBLISH — Payload POST skipped (dry-run)\n"
                    f"title: {generated.get('title', '')}\n"
                    f"slug:  {slugify(generated.get('slug') or generated.get('title', 'post'))}\n"
                    f"featured_image: {featured_image_url or '(none)'}\n"
                    f"inline_image:   {inline_image_url or '(none)'}",
                )
                success = True
            else:
                # 5. POST to Payload (retries transient failures; may raise)
                try:
                    success = post_to_payload(
                        generated, content_json, featured_image_url, jwt
                    )
                except (TransientHTTPError, NonRetryableHTTPError) as e:
                    log.error(
                        f"Payload publish failed after retries — leaving unseen: {e}"
                    )
                    # Generation already burned Gemini quota — pause before next article
                    time.sleep(3)
                    continue

            # Mark seen on success or permanent publish failure (duplicate slug, 4xx).
            # Transient failures above leave the article unseen for a later run.
            seen.add(article["id"])

            if success:
                published_count += 1

            # Pause only after a real generate_post path (not early skips)
            time.sleep(3)

    if dry_run:
        dry_print(
            "save_seen",
            f"SKIPPED — would have saved {len(seen)} seen article IDs to {STATE_FILE}",
        )
        dry_print(
            "save_feed_stats",
            f"SKIPPED — would have saved quality stats for {len(feed_stats)} "
            f"feed(s) to {FEED_STATS_FILE}",
        )
    else:
        save_seen(seen)
        save_feed_stats(feed_stats)
    log.info(
        f"=== Done. Published {published_count}/{MIN_PUBLISH} target "
        f"({processed_count} articles processed, {len(used_feeds)} feeds polled). ==="
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Xetarev Crawlspace (Agent)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the full pipeline without publishing to Payload or updating "
             "seen_articles.json / feed_quality.json; print verbose diagnostics",
    )
    args = parser.parse_args()
    main(dry_run=args.dry_run)