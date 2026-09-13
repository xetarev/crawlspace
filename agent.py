"""
Xetarev SEO News Agent
-----------------------
Runs on GitHub Actions on a cron schedule.
Pipeline:
  1. Poll RSS feeds for new articles
  2. Filter unseen articles using seen_articles.json
  3. Fetch full article text via trafilatura
  4. Search Unsplash for a relevant image URL
  5. Generate SEO post via Gemini (title, slug, excerpt, body)
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
import httpx
import feedparser
import trafilatura
from google import genai
from google.genai import types
from slugify import slugify
from datetime import datetime, timezone

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ─── Config from GitHub Actions Secrets ────────────────────────────────────
GEMINI_API_KEY    = os.environ["GEMINI_API_KEY"]
PAYLOAD_API_URL   = os.environ["PAYLOAD_API_URL"]   # e.g. https://your-app.vercel.app/api/inlight-posts
PAYLOAD_API_TOKEN = os.environ["PAYLOAD_API_TOKEN"]

# ─── RSS Feeds to poll ──────────────────────────────────────────────────────
# Add or remove feeds here. These are all verified active in 2026.
RSS_FEEDS = [
    # Mainstream Tech News & Analysis
    ("TechCrunch AI",          "https://techcrunch.com/category/artificial-intelligence/feed/"),
    ("WIRED",                  "https://www.wired.com/feed/rss"),
    ("The Verge",              "https://www.theverge.com/rss/index.xml"),
    ("ZDNET",                  "https://www.zdnet.com/news/rss.xml"),
    ("Computerworld",          "https://www.computerworld.com/feed"),
    ("InfoWorld",              "https://www.infoworld.com/feed"),
    ("SiliconANGLE",           "https://siliconangle.com/feed"),
    ("Gizmodo",                "https://gizmodo.com/rss"),
    ("The Register",           "https://search.theregister.com/?q=technology&_t=all&feed=1"),
    
    # AI & Machine Learning
    ("Hugging Face",           "https://huggingface.co/blog/feed.xml"),
    ("Google DeepMind",        "https://deepmind.google/blog/feed"),
    ("OpenAI News",            "https://openai.com/news/rss.xml"),
    ("The Decoder",            "https://the-decoder.com/feed"),
    ("Synced",                 "https://syncedreview.com/feed"),
    ("KDnuggets",              "https://www.kdnuggets.com/feed"),
    ("Towards Data Science",   "https://towardsdatascience.com/feed"),
    ("NVIDIA Developer",       "https://developer.nvidia.com/blog//feed"),
    ("Unite.AI",               "https://www.unite.ai/feed/"),
    ("MarkTechPost",           "https://www.marktechpost.com/feed/"),

    # Consumer Tech & Gadget Reviews
    ("Engadget",               "https://www.engadget.com/rss.xml"),
    ("CNET",                   "https://www.cnet.com/rss/news/"),
    ("PCMag",                  "https://www.pcmag.com/feeds/rss/latest"),
    ("Tom's Hardware",         "https://www.tomshardware.com/feeds.xml"),
    ("Tom's Guide",            "https://www.tomsguide.com/feeds.xml"),
    ("TechRadar",              "https://www.techradar.com/feeds.xml"),
    ("Digital Trends",         "https://www.digitaltrends.com/feed"),
    ("TechSpot",               "https://www.techspot.com/backend.xml"),
    ("Trusted Reviews",        "https://www.trustedreviews.com/feed"),
    ("SlashGear",              "https://www.slashgear.com/feed"),

    # Mobile & Smartphone News
    ("GSMArena",               "https://www.gsmarena.com/rss-news-reviews.php3"),
    ("PhoneArena",             "https://www.phonearena.com/feed/news"),
    ("XDA",                    "https://www.xda-developers.com/feed"),
    ("Android Authority",      "https://www.androidauthority.com/feed"),
    ("Android Central",        "https://feeds.feedburner.com/androidcentral"),
    ("9to5Google",             "https://9to5google.com/feed"),
    ("Android Police",         "https://www.androidpolice.com/feed"),
    ("SamMobile",              "https://www.sammobile.com/feed"),
    ("Android Headlines",      "https://www.androidheadlines.com/feed"),
    ("Pocketnow",              "https://pocketnow.com/feed"),

    # Cybersecurity
    ("Krebs on Security",      "https://krebsonsecurity.com/feed"),
    ("The Hacker News",        "https://feeds.feedburner.com/TheHackersNews"),
    ("BleepingComputer",       "https://www.bleepingcomputer.com/feed"),
    ("Dark Reading",           "https://www.darkreading.com/rss.xml"),
    ("SecurityWeek",           "https://www.securityweek.com/feed"),
    ("CSO Online",             "https://www.csoonline.com/feed"),
    ("Help Net Security",      "https://www.helpnetsecurity.com/feed"),
    ("Schneier on Security",   "https://www.schneier.com/feed"),
    ("Sophos",                 "https://news.sophos.com/en-us/feed/"),
    
    # Software Developer & Programming
    ("Y Combinator Hacker News","https://news.ycombinator.com/rss"),
    ("GitHub Blog",            "https://github.blog/feed"),
    ("Stack Overflow Blog",    "https://stackoverflow.blog/feed"),
    ("InfoQ",                  "https://feed.infoq.com"),
    ("The New Stack",          "https://thenewstack.io/feed"),
    ("Smashing Magazine",      "https://www.smashingmagazine.com/feed"),
    ("CSS-Tricks",             "https://css-tricks.com/feed"),
    ("freeCodeCamp",           "https://www.freecodecamp.org/news/rss"),
    ("DEV Community",          "https://dev.to/feed"),
    ("SitePoint",              "https://www.sitepoint.com/sitepoint.rss"),

    # Apple Ecosystem
    ("9to5Mac",                "https://9to5mac.com/feed"),
    ("MacRumors",              "https://feeds.macrumors.com/MacRumors-All"),
    ("AppleInsider",            "https://appleinsider.com/rss/news"),
    ("Macworld",               "https://www.macworld.com/index.rss"),
    ("iMore",                  "https://www.imore.com/rss.xml"),
    ("iLounge",                "https://www.ilounge.com/feed"),

    # Enterprise, IT & Cloud
    ("MIT Technology Review",  "https://www.technologyreview.com/feed"),
    ("TechRepublic",            "https://www.techrepublic.com/rssfeeds/articles/"),
    ("IEEE Spectrum",          "https://feeds.feedburner.com/IeeeSpectrum"),
    ("O'Reilly",               "https://feeds.feedburner.com/oreilly/radar"),
    ("Redapt",                 "https://redapt.com/blog/rss.xml"),
    
    # Startups, Venture & Business of Tech
    ("TechCrunch Startups",    "https://techcrunch.com/category/startups/feed/"),
    ("VentureBeat",             "https://feeds.feedburner.com/venturebeat/SZYF"),
    ("Tech.eu",                "https://tech.eu/feed"),
    ("GeekWire",               "https://geekwire.com/feed"),
    ("Silicon Republic",       "https://www.siliconrepublic.com/feed"),
    ("Vulcan Post",            "https://vulcanpost.com/feed"),
    ("Irish Tech News",        "https://irishtechnews.ie/feed"),
    
    # Culture, Opinion & Analysis
    # ("Vox Technology",         "https://www.vox.com/rss/technology/index.xml"),
    # ("The Next Web",           "https://feeds.feedburner.com/thenextweb"),
    # ("Slashdot",               "https://rss.slashdot.org/Slashdot/slashdotMain"),
    # ("Techdirt",               "https://feeds.feedburner.com/techdirt"),
    # ("Firstpost Tech",         "https://www.firstpost.com/commonfeeds/v1.0/tech.xml"),
    # ("HuffPost Tech",          "https://www.huffpost.com/section/technology/feed"),

    # Tutorials, How-To & Explainers
    ("MakeUseOf",               "https://www.makeuseof.com/feed/category/technology-explained/"),
    ("gHacks",                 "https://www.ghacks.net/feed"),
    ("Techopedia",             "https://www.techopedia.com/feed"),
    ("Fossbytes",              "https://fossbytes.com/feed/?x=1"),
    ("Teacher Tech",           "https://alicekeeler.com/feed"),
    ("How-To Geek",            "https://www.howtogeek.com/feed/"),
    ("BetaNews",               "https://betanews.com/feed/"),
    
    # Science & Emerging Tech
    ("Ars Technica",           "https://feeds.arstechnica.com/arstechnica/index"),
    ("New Scientist",          "https://www.newscientist.com/feed/home/"),
    ("Quanta Magazine",        "https://www.quantamagazine.org/feed"),
    ("Live Science",           "https://www.livescience.com/feeds/all"),
    ("Phys.org",               "https://phys.org/rss-feed/physics-news"),
    ("Tech Xplore",            "https://techxplore.com/rss-feed"),
    ("ScienceDaily",           "https://www.sciencedaily.com/rss/all.xml"),
    ("Interesting Engineering","https://interestingengineering.com/feed"),
    ("Futurism",               "https://futurism.com/feed"),
    
    # IT Services, Consulting & Vendor Blogs
    ("ISHIR",                 "https://www.ishir.com/feed"),
    ("Office1",               "https://office1.com/blog/rss.xml"),
    ("Tech Research Online",   "https://techresearchonline.com/blog/feed/"),
]

# ─── Agent settings ──────────────────────────────────────────────────────────
ARTICLES_PER_RUN   = 4    # how many new posts to create per GitHub Actions run
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
You are a content writer for Xetarev, an indie tech company.

About Xetarev:
- 6-year-old indie tech company, started as a tech blog in 2020
- Ships its own software products (Apps division) and takes boutique client work (Studio)
- Unincorporated but operates as a serious, defined brand
- Xetarev Studio covers full product lifecycle: design, development, strategy
- Studio is selective and intentional — not a volume agency

Voice and angle:
- Practical and skeptical of hype — always ask "does this actually matter?"
- Opinionated but grounded. We respect craft and good engineering.
- We are indie, so we naturally root for independent creators, small teams, and builders
- We find large corporate moves interesting but call out spin when we see it
- Tone: sharp, informed, direct. Not casual blog-speak. Not corporate press release.
- Write like a smart founder commenting on the industry, not a journalist covering it.
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

def fetch_new_articles(seen: set) -> list[dict]:
    """
    Shuffle feeds, poll ARTICLES_PER_RUN of them, return one unseen article per feed.
    """
    feeds = list(RSS_FEEDS)
    random.shuffle(feeds)
    selected = feeds[:ARTICLES_PER_RUN]
    log.info(f"Selected {len(selected)} feeds this run: {[name for name, _ in selected]}")

    candidates = []
    for source_name, feed_url in selected:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                uid = article_id(entry)
                if uid in seen:
                    continue
                candidates.append({
                    "id":      uid,
                    "source":  source_name,
                    "title":   entry.get("title", "").strip(),
                    "url":     entry.get("link", "").strip(),
                    "summary": entry.get("summary", "").strip(),
                })
                break  # one candidate per feed
        except Exception as e:
            log.warning(f"Failed to fetch feed {feed_url}: {e}")

    log.info(f"Found {len(candidates)} unseen articles across selected feeds")
    return candidates


# ─── Full text extraction ─────────────────────────────────────────────────────

def get_article_text(article: dict) -> str:
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
        return clean_summary

    log.info(f"Summary too short ({len(clean_summary)} chars), fetching full text: {article['url']}")
    try:
        downloaded = trafilatura.fetch_url(article["url"])
        if downloaded:
            text = trafilatura.extract(
                downloaded,
                include_formatting=False,
                include_comments=False,
                no_fallback=False,
            )
            if text and len(text) > len(clean_summary):
                log.info(f"trafilatura extracted {len(text)} chars")
                return text[:4000]  # cap to avoid burning Gemini tokens
    except Exception as e:
        log.warning(f"trafilatura failed for {article['url']}: {e}")

    log.info("Falling back to RSS summary")
    return clean_summary or article.get("title", "")


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


def find_image_url(query: str) -> str:
    """
    Search DuckDuckGo Images for a relevant image URL.
    Tries each result until one is accessible via HEAD.
    Falls back to Picsum if search fails or none are accessible.
    """
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
            if image and is_image_accessible(image):
                log.info(f"DDG image found (accessible): {image[:80]}")
                return image
    except Exception as e:
        log.warning(f"DDG image search failed: {e}")

    # Fallback
    import hashlib
    seed_int = int(hashlib.md5(query.encode()).hexdigest(), 16) % 1000
    return f"https://picsum.photos/seed/{seed_int}/1200/630"


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


def generate_post(article: dict, body_text: str) -> dict | None:
    """
    Call Gemini to produce a structured JSON response with all post fields.
    Tries each model in MODEL_CHAIN until one succeeds.
    Returns a dict with: title, slug, excerpt, paragraphs (list of strings), keywords (list)
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

Instructions:
- Write a fresh, original post. Do NOT copy sentences from the source. Rewrite everything.
- Length: 350–500 words across 4–6 paragraphs.
- Write objectively throughout. Do not insert Xetarev opinions or brand voice into the body.
- The final paragraph only should include a single, natural, non-pushy sentence that briefly references a relevant Xetarev service (Studio for client/product work, Apps for software products). Only mention it if it fits organically: do not force it.
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
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=types.GenerateContentConfig(max_output_tokens=1000),
            )
            raw = response.text.strip()
            # Strip markdown code fences if Gemini adds them despite instructions
            raw = re.sub(r"^```json\s*", "", raw)
            raw = re.sub(r"^```\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            data = json.loads(raw)
            log.info(f"Gemini ({model_name}) generated post: {data.get('title', '')[:60]}")
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
        "category": "aif",
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

def main():
    log.info("=== Xetarev SEO News Agent starting ===")

    jwt = get_payload_jwt()
    if not jwt:
        log.error("Could not obtain Payload JWT. Exiting.")
        return

    seen = load_seen()
    log.info(f"Loaded {len(seen)} previously seen article IDs")

    articles = fetch_new_articles(seen)
    if not articles:
        log.info("No new articles found this run. Exiting.")
        return

    published_count = 0

    for article in articles:
        log.info(f"--- Processing: {article['title'][:70]} ---")

        # 1. Get article text
        body_text = get_article_text(article)
        if not body_text:
            log.warning("No usable text, skipping.")
            seen.add(article["id"])
            continue

        # 2. Generate post content via Gemini
        generated = generate_post(article, body_text)
        if not generated:
            log.warning("Gemini generation failed, skipping.")
            seen.add(article["id"])
            continue

        # 3. Find images — one for featured, one for inline body
        # Use the post title/keywords as the search query
        image_query = " ".join(generated.get("keywords", [])[:3]) or article["title"][:40]
        featured_image_url = find_image_url(image_query)
        inline_image_url = find_image_url(image_query)

        # 4. Build Lexical content JSON
        content_json = build_lexical_content(
            paragraphs=generated.get("paragraphs", []),
            image_url=inline_image_url,
            image_alt=generated["title"],
        )

        # 5. POST to Payload
        success = post_to_payload(generated, content_json, featured_image_url, jwt)

        # 6. Mark as seen regardless of publish success
        # (so a broken post doesn't retry and spam your CMS)
        seen.add(article["id"])

        if success:
            published_count += 1

        # Respect Gemini free tier rate limit — pause between articles
        # At 3 articles per run this is negligible but good practice
        time.sleep(3)

    save_seen(seen)
    log.info(f"=== Done. Published {published_count}/{len(articles)} posts this run. ===")


if __name__ == "__main__":
    main()