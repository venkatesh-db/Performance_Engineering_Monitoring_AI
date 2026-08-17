#!/bin/bash
# Run in a second terminal during any load test.
while true; do
  clear
  echo "===== $(date +%T) ====="
  echo "-- Postgres connections --"
  psql -h localhost -U postgres -d payments -tAc \
    "SELECT state, count(*) FROM pg_stat_activity WHERE datname='payments' GROUP BY 1;" 2>/dev/null
  echo "-- Waiting on locks --"
  psql -h localhost -U postgres -d payments -tAc \
    "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock';" 2>/dev/null
  echo "-- Redis --"
  redis-cli INFO stats 2>/dev/null | grep -E "keyspace_(hits|misses)"
  echo "-- API config --"
  curl -s localhost:8000/config
  sleep 2
done
