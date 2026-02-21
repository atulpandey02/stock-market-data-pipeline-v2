#!/bin/bash
# =============================================================================
# setup_and_push.sh
# Creates the GitHub repo and pushes the v2 project in one shot.
# Usage: bash setup_and_push.sh
# =============================================================================

set -e

GITHUB_TOKEN="YOUR_GITHUB_TOKEN"
GITHUB_USER="atulpandey02"
REPO_NAME="stock-market-data-pipeline-v2"
REPO_DESC="Stock Market Data Pipeline v2 — with dbt transformation layer for versioned, tested analytics in Snowflake"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Step 1: Creating GitHub repo '$REPO_NAME'..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

curl -s -X POST \
  -H "Authorization: token $GITHUB_TOKEN" \
  -H "Accept: application/vnd.github.v3+json" \
  https://api.github.com/user/repos \
  -d "{
    \"name\": \"$REPO_NAME\",
    \"description\": \"$REPO_DESC\",
    \"private\": false,
    \"auto_init\": false
  }"

echo ""
echo "✅ Repo created: https://github.com/$GITHUB_USER/$REPO_NAME"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Step 2: Initialising local repo..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Navigate to the project folder (adjust path if needed)
cd "$(dirname "$0")"

git init
git config user.email "atulpandey02@users.noreply.github.com"
git config user.name "$GITHUB_USER"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Step 3: Staging all files..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

git add .
git status --short

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Step 4: Initial commit..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

git commit -m "feat: initial v2 — full pipeline + dbt transformation layer

- Kafka → Spark → MinIO → Snowflake pipeline (from v1)
- dbt staging models: stg_daily_stock_metrics, stg_realtime_stock_analytics
- dbt intermediate: int_daily_returns, int_rolling_metrics, int_realtime_enriched
- dbt marts: mart_stock_performance, mart_daily_summary, mart_realtime_signals
- dbt tests: schema tests + 3 custom singular tests
- dbt macros: safe_divide, generate_schema_name
- Airflow dbt_transformation_dag wired into batch pipeline
- GenAI-ready signal_summary field in mart_realtime_signals
- Full column-level documentation in all schema.yml files"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Step 5: Pushing to GitHub..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

git remote add origin "https://$GITHUB_TOKEN@github.com/$GITHUB_USER/$REPO_NAME.git"
git branch -M main
git push -u origin main

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " ✅ All done!"
echo " 🔗 https://github.com/$GITHUB_USER/$REPO_NAME"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
