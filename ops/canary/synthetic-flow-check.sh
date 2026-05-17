#!/usr/bin/env bash
set -euo pipefail

PROM_URL="${PROM_URL:-http://localhost:9090}"
WINDOW="${WINDOW:-5m}"

query() {
  local q="$1"
  curl -sG "$PROM_URL/api/v1/query" --data-urlencode "query=$q"
}

echo "[canary] checking recent pipeline activity over $WINDOW"

TICKS=$(query "sum(increase(business_ticks_published_total[$WINDOW]))")
UPDATES=$(query "sum(increase(business_price_updates_published_total[$WINDOW]))")
TASKS=$(query "sum(increase(business_portfolio_calc_tasks_created_total[$WINDOW]))")
SNAPSHOTS=$(query "sum(increase(business_portfolio_snapshots_published_total[$WINDOW]))")
DELIVERED=$(query "sum(increase(business_updates_delivered_total[$WINDOW]))")

printf "ticks=%s\nupdates=%s\ntasks=%s\nsnapshots=%s\ndelivered=%s\n" "$TICKS" "$UPDATES" "$TASKS" "$SNAPSHOTS" "$DELIVERED"

echo "[canary] PASS when all counters show non-zero increases in active demo mode"
