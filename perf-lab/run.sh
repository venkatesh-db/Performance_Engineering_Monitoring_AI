#!/bin/bash
# usage: ./run.sh <plan> <label> [-Jkey=val ...]
set -e
LAB="$(cd "$(dirname "$0")" && pwd)"
JM="${JM:-jmeter}"
PLAN=$1; LABEL=$2; shift 2

mkdir -p "$LAB/results" "$LAB/reports"
rm -rf "$LAB/reports/$LABEL" "$LAB/results/$LABEL.jtl"

echo "=== $LABEL | $(date -u +%FT%TZ) ===" | tee -a "$LAB/results/timeline.txt"

"$JM" -n -t "$LAB/plans/$PLAN.jmx" \
      -l "$LAB/results/$LABEL.jtl" \
      -e -o "$LAB/reports/$LABEL" \
      -Jdatadir="$LAB/data" -Jrunid="$LABEL" "$@"

echo "=== $LABEL END | $(date -u +%FT%TZ) ===" | tee -a "$LAB/results/timeline.txt"
echo ""
echo "Report: $LAB/reports/$LABEL/index.html"
command -v open >/dev/null && open "$LAB/reports/$LABEL/index.html" || true
