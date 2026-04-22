name: Daily Comtrade Data Fetch

on:
  schedule:
    - cron: "0 4 * * *"
  workflow_dispatch:

env:
  FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true

jobs:
  fetch-comtrade:
    runs-on: ubuntu-latest
    permissions:
      contents: write

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install requests

      - name: Check for Comtrade API key
        run: |
          if [ -z "${{ secrets.COMTRADE_API_KEY }}" ]; then
            echo "⚠ WARNING: COMTRADE_API_KEY secret not set"
            echo "Partner/commodity data will NOT be updated"
            echo "Register free at: https://comtradedeveloper.un.org"
            echo "Then add as GitHub Secret: COMTRADE_API_KEY"
          else
            echo "✓ COMTRADE_API_KEY is set"
          fi

      - name: Run daily fetcher
        env:
          COMTRADE_API_KEY: ${{ secrets.COMTRADE_API_KEY }}
        run: python fetch_comtrade_daily.py
        timeout-minutes: 25

      - name: Validate output
        run: |
          python - << 'EOF'
          import json
          with open("data.json") as f:
              d = json.load(f)
          n = len(d["countries"])
          updated = d["_meta"].get("countries_updated_today", 0)
          reqs = d["_meta"].get("requests_used_today", 0)
          print(f"✓ {n} total countries, {updated} updated today, {reqs} requests used")
          assert n > 0
          EOF

      - name: Commit if changed
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git diff --quiet data.json || (
            git add data.json &&
            git commit -m "data: daily update $(date -u +'%Y-%m-%d')" &&
            git push
          )

      - name: Upload artifact
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: trade-data-${{ github.run_id }}
          path: data.json
          retention-days: 7
