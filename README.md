# Xetarev Crawlspace

Automated SEO content pipeline for the Xetarev Inlight blog.

Runs on GitHub Actions every hour. Polls tech news RSS feeds, generates original posts via Gemini, and publishes directly to Payload CMS.

## Stack

| Layer | Tool |
|---|---|
| Scheduler | GitHub Actions cron |
| RSS parsing | feedparser |
| Full-text extraction | trafilatura |
| AI generation | Google Gemini 2.0 Flash-Lite (free tier) |
| Images | Unsplash Source (free, no key) |
| CMS | Payload REST API |

## Setup

### 1. Add GitHub Actions Secrets

In this repo: **Settings → Secrets and variables → Actions → New repository secret**

| Secret name | Value |
|---|---|
| `GEMINI_API_KEY` | Your Google AI Studio API key |
| `PAYLOAD_API_URL` | `https://your-app.vercel.app/api/inlight-posts` |
| `PAYLOAD_API_TOKEN` | Your Payload API token (create in Payload admin) |

### 2. Test manually

Go to **Actions tab → Xetarev SEO News Agent → Run workflow**

Check the logs. If it posts successfully, the cron will handle the rest.

## Configuration

Edit `agent.py` to:
- Add/remove RSS feeds in `RSS_FEEDS`
- Change `ARTICLES_PER_RUN` (default: 3 per hourly run)
- Adjust `BRAND_CONTEXT` if the voice needs updating
- Change `GEMINI_MODEL` if you want to try a different free model

## Frequency

Default: every hour (`0 * * * *`). Change in `.github/workflows/agent.yml`.

## How deduplication works

`seen_articles.json` stores MD5 hashes of every processed article URL. The workflow commits this file back to the repo after each run with `[skip ci]` in the commit message to prevent triggering another workflow run.