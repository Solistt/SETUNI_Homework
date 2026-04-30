#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/output"
FILES_TO_SHOW="${1:-2}"

if [[ ! -d "$OUTPUT_DIR" ]]; then
  echo "Output directory does not exist: $OUTPUT_DIR"
  exit 1
fi

shopt -s nullglob
files=("$OUTPUT_DIR"/tweets_*.csv)
shopt -u nullglob

if [[ ${#files[@]} -eq 0 ]]; then
  echo "No generated files found in $OUTPUT_DIR"
  exit 0
fi

echo "Generated files:"
for file in "${files[@]}"; do
  basename "$file"
done

echo ""
echo "Showing content from up to $FILES_TO_SHOW file(s):"

shown=0
for file in "${files[@]}"; do
  echo ""
  echo "=== $(basename "$file") ==="
  head -n 20 "$file"
  shown=$((shown + 1))
  if [[ "$shown" -ge "$FILES_TO_SHOW" ]]; then
    break
  fi
done
