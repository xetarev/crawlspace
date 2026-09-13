name: Xetarev Crawlspace (Agent)

on:
  schedule:
    # Runs every hour at :00 UTC — 24 posts/day max
    # Change */2 for every 2 hours, etc.
    - cron: '0 * * * *'
  workflow_dispatch:
    # Lets you trigger a manual run from the Actions tab in GitHub
    # Use this to test before the first scheduled run fires

jobs:
  run-agent:
    runs-on: ubuntu-latest

    permissions:
      contents: write  # Required so the job can commit seen_articles.json

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'
          cache: 'pip'  # GitHub caches pip deps between runs — speeds up cold starts

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run agent
        env:
          GEMINI_API_KEY:    ${{ secrets.GEMINI_API_KEY }}
          PAYLOAD_API_URL:   ${{ secrets.PAYLOAD_API_URL }}
          PAYLOAD_API_TOKEN: ${{ secrets.PAYLOAD_API_TOKEN }}
        run: python agent.py

      - name: Commit updated seen_articles.json
        run: |
          git config user.name "xetarev-agent[bot]"
          git config user.email "xetarev-agent[bot]@users.noreply.github.com"
          git add seen_articles.json
          # Only commit if the file actually changed — avoids empty commits
          git diff --cached --quiet || git commit -m "chore: update seen articles [skip ci]"
          git push