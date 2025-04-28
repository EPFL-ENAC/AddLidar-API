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

# --- helpers ---------------------------------------------------------------
rand_name() { LC_ALL=C </dev/urandom tr -dc 'a-z0-9' | head -c 8; printf '\n'; }
rand_size() { echo $(( (RANDOM % (MAX_SIZE_KB-1)) + 1 )); }
have_pigz() { command -v pigz >/dev/null 2>&1; }

rm -rf -- "$ORIG_ROOT" "$ZIP_ROOT"
mkdir -p  "$ORIG_ROOT" "$ZIP_ROOT"

echo "➜ Creating tree under $ORIG_ROOT"
for ((i=1;i<=LEVEL1_COUNT;i++)); do
  L1="lvl1_$(printf %02d "$i")"
  for ((j=1;j<=LEVEL2_PER_L1;j++)); do
    L2="lvl2_$(printf %02d "$j")"
    BASE="$ORIG_ROOT/$L1/$L2"
    for ((k=1;k<=DEPTH3_COUNT;k++)); do
      L3="d3_$(printf %02d "$k")"
      for ((m=1;m<=DEPTH4_COUNT;m++)); do
        L4="d4_$(printf %02d "$m")"
        D="$BASE/$L3/$L4"; mkdir -p "$D"
        for ((f=1;f<=FILES_PER_D4;f++)); do
          SZ=$(rand_size)
          dd if=/dev/urandom of="$D/$(rand_name).bin" bs=1k count="$SZ" status=none
        done
      done
    done
  done
done

echo "➜ Creating archives under $ZIP_ROOT"
compress_flag="-czf"                   # default = gzip
compress_prog=""                       # Standard tar with gzip compression

# # Clear existing ZIP_ROOT but preserve the directory
# rm -rf "${ZIP_ROOT:?}"/*
# mkdir -p "$ZIP_ROOT"

# # Find directories at level 2 (lvl1_XX/lvl2_XX) and create archives
# find "$ORIG_ROOT" -type d | awk -v ORIG="$ORIG_ROOT" '
#   BEGIN { n=split(ORIG,_,"/") }
#   {
#     depth = split($0, path_parts, "/") - n
#     if (depth == 2) print
#   }' | while read -r DIR; do
#   REL="${DIR#$ORIG_ROOT/}"
#   DEST="$ZIP_ROOT/$REL.tar.gz"
#   mkdir -p "$(dirname "$DEST")"
#   echo "Creating archive: $DEST from $REL"
#   # Create archive at the original root and include the relative path
#   tar $compress_flag -C "$ORIG_ROOT" -f "$DEST" "$REL"
# done

# printf '✓ Created %d .tar.gz files\n' "$(find "$ZIP_ROOT" -name '*.tar.gz' | wc -l | tr -d ' ')"