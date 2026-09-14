"""
Xetarev SEO News Agent
-----------------------
Runs on GitHub Actions on a cron schedule.
Pipeline:
  1. Poll RSS feeds for new articles
  2. Filter unseen articles using seen_articles.json
  3. Fetch full article text via trafilatura
  4. Prefer featured image from RSS feed data; DuckDuckGo / Picsum as fallback
  5. Generate SEO post via Gemini (title, slug, excerpt, body) — or skip if off-topic
  6. Build Payload Lexical JSON content structure
  7. POST to Payload CMS REST API as published
  8. Commit updated seen_articles.json back to repo
"""
import os
import json
import time
import hashlib
import logging
import random
import re
import argparse
import httpx
import feedparser
import trafilatura
from dotenv import load_dotenv
from google import genai
from google.genai import types
from slugify import slugify
from datetime import datetime, timezone
from ai_text_audit import Auditor
from ai_text_audit.cli import format_terminal
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
def dry_print(label: str, content: str = "") -> None:
    """Print clearly labeled dry-run output so it stands out in the terminal."""
    sep = "─" * 72
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
MIN_SUMMARY_LENGTH = 300   # chars — below this we try trafilatura for full text
MODEL_CHAIN = [
    "gemini-3.5-flash-lite",
    "gemini-3.1-flash-lite",
    "gemini-3.5-flash",
    "gemini-3.6-flash",
    "gemini-3.7-flash",
]
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
def fetch_new_articles(seen: set, used_feeds: set | None = None, dry_run: bool = False) -> list[dict]:
    """
    Shuffle unused feeds, poll up to ARTICLES_PER_RUN of them, return one unseen
    article per feed. used_feeds tracks feed URLs already polled this run.
    """
    used = used_feeds if used_feeds is not None else set()
    available = [(name, url) for name, url in RSS_FEEDS if url not in used]
    if not available:
        log.info("No unused feeds left to poll")
        if dry_run:
            dry_print("Feed selection", "No unused feeds left to poll")
        return []

    random.shuffle(available)
    selected = available[:ARTICLES_PER_RUN]
    log.info(f"Selected {len(selected)} feeds this batch: {[name for name, _ in selected]}")
    if dry_run:
        lines = [f"  - {name} ({url})" for name, url in selected]
        dry_print("Feeds selected this batch", "\n".join(lines) if lines else "(none)")

    candidates = []
    for source_name, feed_url in selected:
        used.add(feed_url)
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                uid = article_id(entry)
                if uid in seen:
                    continue
                feed_image = extract_feed_image(entry)
                candidates.append({
                    "id":      uid,
                    "source":  source_name,
                    "title":   entry.get("title", "").strip(),
                    "url":     entry.get("link", "").strip(),
                    "summary": entry.get("summary", "").strip(),
                    "image":   feed_image,
                })
                break  # one candidate per feed
        except Exception as e:
            log.warning(f"Failed to fetch feed {feed_url}: {e}")

    log.info(f"Found {len(candidates)} unseen articles across selected feeds")
    return candidates
# ─── Full text extraction ─────────────────────────────────────────────────────
def get_article_text(article: dict, dry_run: bool = False) -> str:
    """
    Return the best available text for an article.
    Uses RSS summary if long enough, otherwise fetches full text via trafilatura.
    Falls back to summary if trafilatura fails or is blocked.
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
        try:
            downloaded = trafilatura.fetch_url(article["url"])
            if downloaded:
                extracted = trafilatura.extract(
                    downloaded,
                    include_formatting=False,
                    include_comments=False,
                    no_fallback=False,
                )
                if extracted and len(extracted) > len(clean_summary):
                    log.info(f"trafilatura extracted {len(extracted)} chars")
                    text = extracted[:4000]  # cap to avoid burning Gemini tokens
        except Exception as e:
            log.warning(f"trafilatura failed for {article['url']}: {e}")
        if text is None:
            log.info("Falling back to RSS summary")
            text = clean_summary or article.get("title", "")
    if dry_run:
        dry_print(
            "Extracted article text (before Gemini)",
            text if text else "(empty)",
        )
    return text
# ─── Image search ─────────────────────────────────────────────────────────────
def is_image_accessible(url: str) -> bool:
    """Return True if a HEAD request to url returns status 200."""
    try:
        r = httpx.head(
            url,
            timeout=5,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; Xetarev-Agent/1.0)"},
        )
        return r.status_code == 200
    except Exception:
        return False
def find_image_url(
    query: str,
    exclude_urls: set[str] | None = None,
    dry_run: bool = False,
    dry_label: str = "Image search",
) -> str:
    """
    Search DuckDuckGo Images for a relevant image URL.
    Tries each result until one is accessible via HEAD.
    Skips any URL in exclude_urls (e.g. already chosen as featured).
    Falls back to Picsum if search fails or none are accessible.
    """
    exclude = exclude_urls or set()
    try:
        # DuckDuckGo image search vqd token
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
        for result in results:
            image = result.get("image", "")
            if not image or image in exclude:
                continue
            if is_image_accessible(image):
                log.info(f"DDG image found (accessible): {image[:80]}")
                if dry_run:
                    dry_print(
                        dry_label,
                        f"URL: {image}\nAccessibility check: PASSED",
                    )
                return image
    except Exception as e:
        log.warning(f"DDG image search failed: {e}")
    # Fallback — offset seed when excluding so featured/inline stay distinct
    seed_int = int(hashlib.md5(query.encode()).hexdigest(), 16) % 1000
    if exclude:
        seed_int = (seed_int + 1) % 1000
    fallback = f"https://picsum.photos/seed/{seed_int}/1200/630"
    if fallback in exclude:
        seed_int = (seed_int + 1) % 1000
        fallback = f"https://picsum.photos/seed/{seed_int}/1200/630"
    if dry_run:
        dry_print(
            dry_label,
            f"URL: {fallback}\nAccessibility check: FAILED / N/A "
            f"(Picsum fallback — no accessible DDG result)",
        )
    return fallback
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
def generate_post(article: dict, body_text: str, dry_run: bool = False) -> dict | None:
    """
    Call Gemini to produce a structured JSON response with all post fields.
    Tries each model in MODEL_CHAIN until one succeeds.
    Returns a dict with: title, slug, excerpt, paragraphs (list of strings), keywords (list)
    May return {"skip": true} when the source is off-topic.
    Returns None if generation fails.
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
Relevance gate (do this first):
- The source must be a strong fit for: technology, software, AI, startups, developer tools, cybersecurity, or indie building.
- If it is not a strong fit, respond ONLY with: {{"skip": true}}
- Do not write a post for weak or tangential topics (lifestyle fluff, unrelated politics, pure entertainment, education-for-teachers, generic vendor marketing, etc.).
Instructions (only if relevant):
- Write a fresh, original post. Do NOT copy sentences from the source. Rewrite everything.
- Length: 350–500 words across 4–6 paragraphs.
- Voice: knowledgeable individual with a point of view. First person where natural. Direct opinions. Sharp, informed, human — not an AI content pipeline.
- End the post naturally. Do NOT mention Xetarev, Xetarev Studio, or Xetarev Apps.
- SEO-optimized: use the main topic keyword naturally in the title and early in the body.
- The excerpt should be 1–2 sentences, punchy, suitable for a feed preview.
- Paragraph 3 or 4 should include [IMAGE] as a placeholder on its own line — this is where the inline image will be inserted.
Respond ONLY with valid JSON, no markdown fences, no extra text. Format when writing a post:
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
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=types.GenerateContentConfig(max_output_tokens=1000),
            )
            raw = response.text.strip()
            if dry_run:
                dry_print("Raw Gemini response (before JSON parsing)", raw)
            # Strip markdown code fences if Gemini adds them despite instructions
            raw = re.sub(r"^```json\s*", "", raw)
            raw = re.sub(r"^```\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            data = json.loads(raw)
            if data.get("skip"):
                log.info(f"Gemini ({model_name}) skipped article as off-topic: {article['title'][:60]}")
            else:
                log.info(f"Gemini ({model_name}) generated post: {data.get('title', '')[:60]}")
                if dry_run:
                    paragraphs = data.get("paragraphs") or []
                    para_block = "\n".join(
                        f"  [{i}] {p}" for i, p in enumerate(paragraphs, start=1)
                    )
                    dry_print(
                        "Parsed post fields",
                        f"title: {data.get('title', '')}\n"
                        f"slug: {data.get('slug', '')}\n"
                        f"excerpt: {data.get('excerpt', '')}\n"
                        f"keywords: {data.get('keywords', [])}\n"
                        f"paragraphs ({len(paragraphs)}):\n{para_block}",
                    )
            return data
        except json.JSONDecodeError as e:
            log.error(f"Gemini ({model_name}) returned invalid JSON: {e}\nRaw response: {raw[:300]}")
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

_AI_VOCAB_REPLACEMENTS: dict[str, str] = {
    "delve": "look into",
    "crucial": "important",
    "landscape": "field",
    "leverage": "use",
    "multifaceted": "complex",
    "comprehensive": "thorough",
    "facilitate": "help",
    "streamline": "simplify",
    "harness": "use",
    "underscore": "highlight",
    "illuminate": "show",
    "embark": "start",
    "foster": "build",
    "endeavor": "effort",
    "tapestry": "mix",
    "showcase": "show",
    "pivotal": "key",
    "bolster": "strengthen",
    "nuanced": "subtle",
    "robust": "strong",
    "paradigm": "model",
    "synergy": "teamwork",
    "holistic": "overall",
    "myriad": "many",
    "plethora": "plenty",
    "groundbreaking": "new",
    "cutting-edge": "latest",
    "revolutionary": "major",
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


def _humanizer_skill_pattern_pass(text: str) -> str:
    """
    Deterministic cleanup pass mirroring the humanizer-skill pattern catalog:
    hollow openers, hedging, AI vocabulary, em dashes, and leftover filler.
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

    for word, replacement in _AI_VOCAB_REPLACEMENTS.items():
        pattern = re.compile(r"\b" + re.escape(word) + r"\b", re.IGNORECASE)

        def _swap(match: re.Match, _repl: str = replacement) -> str:
            return _case_aware_replace(match.group(0), _repl)

        rewrite = pattern.sub(_swap, rewrite)

    if "—" in rewrite or "–" in rewrite:
        rewrite = (
            rewrite.replace(" — ", ", ")
            .replace("—", ", ")
            .replace(" – ", ", ")
            .replace("–", ", ")
        )

    rewrite = re.sub(r" {2,}", " ", rewrite)
    rewrite = re.sub(r" ([,.!?;:])", r"\1", rewrite)
    rewrite = re.sub(r"\n{3,}", "\n\n", rewrite)
    return rewrite.strip()


def humanize_and_audit_paragraphs(
    paragraphs: list[str], dry_run: bool = False
) -> list[str]:
    """
    Four-stage pipeline on generated paragraphs:
      1. AI audit (baseline score)
      2. TextHumanize
      3. Humanizer-skill pattern catalog pass
      4. AI audit (post-humanization score + delta)
    Failed stages are logged and skipped; remaining text continues.
    """
    paras = list(paragraphs or [])
    baseline_score: float | None = None
    final_score: float | None = None

    # Stage 1 — AI Audit (baseline)
    try:
        result = Auditor().analyze(_join_paragraphs(paras))
        baseline_score = float(result.score)
        log.info(
            f"AI audit baseline: score={result.score} verdict={result.verdict} "
            f"patterns={len(result.patterns)}"
        )
        for p in result.patterns:
            log.info(
                f"  pattern={p.name} severity={p.severity} count={p.count}"
            )
        if dry_run:
            dry_print(
                "Stage 1 — AI Audit (baseline)",
                format_terminal(result, filepath="generated post", verbose=True),
            )
            dry_print(
                "Stage 1 — paragraph text (after)",
                _join_paragraphs(paras),
            )
    except Exception as e:
        log.error(f"Stage 1 AI audit failed (skipped): {e}")

    # Stage 2 — TextHumanize
    try:
        def _th(para: str) -> str:
            return humanize(para, lang="en").text

        paras = _map_paragraphs(paras, _th)
        log.info("TextHumanize ran")
        if dry_run:
            dry_print(
                "Stage 2 — TextHumanize paragraph text (after)",
                _join_paragraphs(paras),
            )
    except Exception as e:
        log.error(f"Stage 2 TextHumanize failed (skipped): {e}")

    # Stage 3 — Humanizer-Skill pattern pass
    try:
        paras = _map_paragraphs(paras, _humanizer_skill_pattern_pass)
        log.info("Humanizer-Skill pattern pass ran")
        if dry_run:
            dry_print(
                "Stage 3 — Humanizer-Skill pattern pass paragraph text (after)",
                _join_paragraphs(paras),
            )
    except Exception as e:
        log.error(f"Stage 3 Humanizer-Skill pattern pass failed (skipped): {e}")

    # Stage 4 — AI Audit (post-humanization)
    try:
        result = Auditor().analyze(_join_paragraphs(paras))
        final_score = float(result.score)
        log.info(
            f"AI audit final: score={result.score} verdict={result.verdict} "
            f"patterns={len(result.patterns)}"
        )
        for p in result.patterns:
            log.info(
                f"  pattern={p.name} severity={p.severity} count={p.count}"
            )
        if dry_run:
            dry_print(
                "Stage 4 — AI Audit (post-humanization)",
                format_terminal(result, filepath="generated post", verbose=True),
            )
            dry_print(
                "Stage 4 — paragraph text (after)",
                _join_paragraphs(paras),
            )
    except Exception as e:
        log.error(f"Stage 4 AI audit failed (skipped): {e}")

    if baseline_score is not None and final_score is not None:
        delta = final_score - baseline_score
        comparison = (
            f"AI-tell score before: {baseline_score:.1f}\n"
            f"AI-tell score after:  {final_score:.1f}\n"
            f"Delta:                {delta:+.1f}"
        )
        log.info(
            f"AI-tell score {baseline_score:.1f} → {final_score:.1f} ({delta:+.1f})"
        )
        print(f"\n[HUMANIZE] Score comparison\n{comparison}")
        if dry_run:
            dry_print("AI-tell score comparison (before → after)", comparison)
    elif baseline_score is not None or final_score is not None:
        log.info(
            f"AI-tell score comparison incomplete "
            f"(baseline={baseline_score}, final={final_score})"
        )

    return paras


# ─── Lexical JSON builder ─────────────────────────────────────────────────────
def build_lexical_content(paragraphs: list[str], image_url: str, image_alt: str) -> dict:
    """
    Build a Payload Lexical content JSON object from a list of paragraph strings.
    Matches exactly the structure observed in your existing inlight_posts rows:
    - paragraph nodes for text
    - imageBlock custom block nodes for images (using externalUrl)
    Any paragraph that is exactly "[IMAGE]" becomes an imageBlock node.
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
    """Log in to Payload and return a short-lived JWT."""
    login_url = PAYLOAD_API_URL.replace("/api/inlight-posts", "/api/users/login")
    try:
        r = httpx.post(login_url, json={
            "email": os.environ["PAYLOAD_EMAIL"],
            "password": os.environ["PAYLOAD_PASSWORD"],
        }, timeout=10)
        if r.status_code == 200:
            token = r.json().get("token")
            log.info("Payload JWT obtained")
            return token
        log.error(f"Payload login failed {r.status_code}: {r.text[:200]}")
        return None
    except Exception as e:
        log.error(f"Payload login error: {e}")
        return None
def post_to_payload(generated: dict, content_json: dict, featured_image_url: str, jwt: str) -> bool:
    """
    POST the generated post to Payload CMS as a published document.
    Field names match exactly what's in your inlight_posts table.
    Returns True on success, False on failure.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    # Ensure slug is URL-safe
    safe_slug = slugify(generated.get("slug") or generated.get("title", "post"))
    # Check if slug already exists
    check = httpx.get(
        PAYLOAD_API_URL,
        params={"where[slug][equals]": safe_slug, "limit": 1},
        headers={"Authorization": f"JWT {jwt}"},
        timeout=10
    )
    if check.status_code == 200 and check.json().get("totalDocs", 0) > 0:
        log.warning(f"Slug already exists, skipping: {safe_slug}")
        return False
    payload = {
        "title":         generated["title"],
        "slug":          safe_slug,
        "excerpt":       generated.get("excerpt", ""),
        "content":       content_json,
        "featuredImage": {
            "url": featured_image_url,
            "alt": generated["title"],
        },
        "meta": {
            "title":       generated["title"],
            "description": generated.get("excerpt", ""),
        },
        "categories": [5],
        "publishedAt":   now_iso,
        "_status":       "published",
    }
    headers = {
        "Authorization": f"JWT {jwt}",
        "Content-Type":  "application/json",
    }
    try:
        r = httpx.post(PAYLOAD_API_URL, json=payload, headers=headers, timeout=15)
        if r.status_code in (200, 201):
            doc = r.json()
            doc_id = doc.get("doc", {}).get("id") or doc.get("id", "unknown")
            log.info(f"✅ Published post ID {doc_id}: {generated['title'][:60]}")
            return True
        else:
            log.error(f"Payload API returned {r.status_code}: {r.text[:300]}")
            return False
    except Exception as e:
        log.error(f"Payload POST failed: {e}")
        return False
# ─── Main pipeline ────────────────────────────────────────────────────────────
def main(dry_run: bool = False):
    log.info("=== Xetarev SEO News Agent starting ===")
    if dry_run:
        dry_print(
            "Mode",
            "Dry-run enabled — Payload POST and seen_articles.json updates are skipped.\n"
            "Pipeline still fetches feeds, extracts text, calls Gemini, and resolves images.",
        )

    jwt = None
    if not dry_run:
        jwt = get_payload_jwt()
        if not jwt:
            log.error("Could not obtain Payload JWT. Exiting.")
            return

    seen = load_seen()
    log.info(f"Loaded {len(seen)} previously seen article IDs")

    published_count = 0
    processed_count = 0
    used_feeds: set[str] = set()

    while published_count < MIN_PUBLISH:
        articles = fetch_new_articles(seen, used_feeds, dry_run=dry_run)
        if not articles:
            log.info("No more unseen articles available. Stopping.")
            break

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

            # 1. Get article text
            body_text = get_article_text(article, dry_run=dry_run)
            if not body_text:
                log.warning("No usable text, skipping.")
                if dry_run:
                    dry_print(
                        "Outcome",
                        "SKIPPED — no usable article text extracted",
                    )
                seen.add(article["id"])
                continue

            # 2. Generate post content via Gemini
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

            if generated.get("skip"):
                log.info(f"Skipping off-topic article (marked seen): {article['title'][:70]}")
                if dry_run:
                    dry_print(
                        "Outcome",
                        "SKIPPED — Gemini marked article as off-topic (skip: true)",
                    )
                seen.add(article["id"])
                continue

            # 3. Featured image: prefer RSS feed image; DDG / Picsum fallback.
            #    Inline body image always via DDG (Picsum fallback), distinct from featured.
            image_query = " ".join(generated.get("keywords", [])[:3]) or article["title"][:40]
            feed_image = article.get("image")
            feed_accessible = False
            if feed_image:
                feed_accessible = is_image_accessible(feed_image)
            if dry_run:
                dry_print(
                    "Featured image (from RSS)",
                    f"URL: {feed_image or '(none)'}\n"
                    f"Accessibility check: "
                    f"{'PASSED' if feed_accessible else 'FAILED' if feed_image else 'N/A (no RSS image)'}",
                )
            if feed_image and feed_accessible:
                featured_image_url = feed_image
                log.info(f"Using RSS feed image for featured: {feed_image[:80]}")
            else:
                if feed_image:
                    log.info("RSS feed image present but not accessible; falling back to DDG")
                featured_image_url = find_image_url(
                    image_query,
                    dry_run=dry_run,
                    dry_label="Featured image (DDG / Picsum fallback)",
                )
            inline_image_url = find_image_url(
                image_query,
                exclude_urls={featured_image_url},
                dry_run=dry_run,
                dry_label="Inline DDG image",
            )

            # 3b. Humanize + audit generated paragraphs (before Lexical build)
            generated["paragraphs"] = humanize_and_audit_paragraphs(
                generated.get("paragraphs", []),
                dry_run=dry_run,
            )

            # 4. Build Lexical content JSON
            content_json = build_lexical_content(
                paragraphs=generated.get("paragraphs", []),
                image_url=inline_image_url,
                image_alt=generated["title"],
            )

            if dry_run:
                dry_print(
                    "Final Lexical JSON (would be sent to Payload)",
                    json.dumps(content_json, indent=2),
                )
                dry_print(
                    "Outcome",
                    "WOULD PUBLISH — Payload POST skipped (dry-run)\n"
                    f"title: {generated.get('title', '')}\n"
                    f"slug:  {slugify(generated.get('slug') or generated.get('title', 'post'))}\n"
                    f"featured_image: {featured_image_url}\n"
                    f"inline_image:   {inline_image_url}",
                )
                success = True
            else:
                # 5. POST to Payload
                success = post_to_payload(generated, content_json, featured_image_url, jwt)

            # 6. Mark as seen regardless of publish success
            # (so a broken post doesn't retry and spam your CMS)
            seen.add(article["id"])

            if success:
                published_count += 1

            # Respect Gemini free tier rate limit — pause between articles
            time.sleep(3)

    if dry_run:
        dry_print(
            "save_seen",
            f"SKIPPED — would have saved {len(seen)} seen article IDs to {STATE_FILE}",
        )
    else:
        save_seen(seen)
    log.info(
        f"=== Done. Published {published_count}/{MIN_PUBLISH} target "
        f"({processed_count} articles processed, {len(used_feeds)} feeds polled). ==="
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Xetarev SEO News Agent")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the full pipeline without publishing to Payload or updating seen_articles.json; print verbose diagnostics",
    )
    args = parser.parse_args()
    main(dry_run=args.dry_run)