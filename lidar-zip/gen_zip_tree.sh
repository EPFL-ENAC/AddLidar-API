#!/usr/bin/env bash
# ------------------------------------------------------------
# gen_test_tree_mac.sh – BSD/Posix-compatible test corpus generator
# ------------------------------------------------------------
set -euo pipefail

ORIG_ROOT="${1:-./original_root}"
ZIP_ROOT="${2:-./zip_root}"
LEVEL1_COUNT="${LEVEL1_COUNT:-5}"
LEVEL2_PER_L1="${LEVEL2_PER_L1:-10}"   # 5×10 = 50 archives
DEPTH3_COUNT="${DEPTH3_COUNT:-3}"
DEPTH4_COUNT="${DEPTH4_COUNT:-3}"
FILES_PER_D4="${FILES_PER_D4:-4}"
MAX_SIZE_KB="${MAX_SIZE_KB:-30}"

# ---
echo "➜ Creating archives under $ZIP_ROOT"
compress_flag="-czf"                   # default = gzip
compress_prog=""                       # Standard tar with gzip compression

# Clear existing ZIP_ROOT but preserve the directory
rm -rf "${ZIP_ROOT:?}"/*
mkdir -p "$ZIP_ROOT"

# Find directories at level 2 (lvl1_XX/lvl2_XX) and create archives
find "$ORIG_ROOT" -type d | awk -v ORIG="$ORIG_ROOT" '
  BEGIN { n=split(ORIG,_,"/") }
  {
    depth = split($0, path_parts, "/") - n
    if (depth == 2) print
  }' | while read -r DIR; do
  REL="${DIR#$ORIG_ROOT/}"
  DEST="$ZIP_ROOT/$REL.tar.gz"
  mkdir -p "$(dirname "$DEST")"
  echo "Creating archive: $DEST from $REL"
  # Create archive at the original root and include the relative path
  # macOS tar requires specific order of arguments
  tar -C "$ORIG_ROOT" $compress_flag "$DEST" "$REL"
done

printf '✓ Created %d .tar.gz files\n' "$(find "$ZIP_ROOT" -name '*.tar.gz' | wc -l | tr -d ' ')"