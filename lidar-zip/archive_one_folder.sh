#!/bin/bash
# filepath: /home/pierre/Documents/Code/AddLidar-API/lidar-zip/archive_folders.sh
# Refactored script to create a compressed archive for a single folder

# Setup logging
log() {
  local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
  echo "[$timestamp] $1"
}

format_size() {
  # Convert bytes to human-readable format
  local bytes=$1
  if [[ $bytes -lt 1024 ]]; then
    echo "${bytes} B"
  elif [[ $bytes -lt 1048576 ]]; then
    local kb=$(awk "BEGIN {printf \"%.2f\", $bytes/1024}")
    echo "${kb} KB"
  elif [[ $bytes -lt 1073741824 ]]; then
    local mb=$(awk "BEGIN {printf \"%.2f\", $bytes/1048576}")
    echo "${mb} MB"
  else
    local gb=$(awk "BEGIN {printf \"%.2f\", $bytes/1073741824}")
    echo "${gb} GB"
  fi
}

log "Script started"

# Check if correct number of arguments are provided
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 SOURCE_FOLDER OUTPUT_FILE"
  echo "Example: $0 /lidar/customer1/project2 /zips/customer1/project2.tar.gz"
  exit 1
fi

SOURCE_FOLDER="$1"
OUTPUT_FILE="$2"

# Fixed thread count for pigz
COMPRESS_THREADS=8

log "Source folder: $SOURCE_FOLDER"
log "Output file: $OUTPUT_FILE"
log "Using fixed $COMPRESS_THREADS compression threads"

# Create output directory if it doesn't exist
OUTPUT_DIR=$(dirname "$OUTPUT_FILE")
mkdir -p "$OUTPUT_DIR"
log "Created output directory structure"

# Skip if archive already exists
if [ -f "$OUTPUT_FILE" ]; then
  log "Deleting existing archive: $OUTPUT_FILE"
  rm -f "$OUTPUT_FILE"
fi

# Get folder name
FOLDER_NAME=$(basename "$SOURCE_FOLDER")
log "Creating archive for: $FOLDER_NAME"

# Get original size
original_size=$(du -sb "$SOURCE_FOLDER" | cut -f1)
original_size_human=$(format_size $original_size)
log "Original size: $original_size_human"

# Create archive with timing
start_time=$(date +%s)

log "Creating tar and compressing with pigz..."

# Use direct tar-to-pigz approach with fixed thread count
tar -C "$(dirname "$SOURCE_FOLDER")" -cf - "$FOLDER_NAME" | \
pigz -p $COMPRESS_THREADS > "$OUTPUT_FILE"
compression_status=$?

end_time=$(date +%s)
elapsed=$((end_time - start_time))

if [ -f "$OUTPUT_FILE" ] && [ $compression_status -eq 0 ]; then
  compressed_size=$(stat -c%s "$OUTPUT_FILE")
  compressed_size_human=$(format_size $compressed_size)
  
  # Calculate compression ratio and speed
  ratio=$(awk "BEGIN {printf \"%.2f\", $compressed_size * 100 / $original_size}")
  speed=$(awk "BEGIN {printf \"%.2f\", $original_size / 1048576 / $elapsed}")
  
  log "✅ Archive created: $OUTPUT_FILE"
  log "   Time taken: ${elapsed}s"
  log "   Original size: $original_size_human"
  log "   Compressed size: $compressed_size_human (${ratio}% of original)"
  log "   Compression speed: ${speed} MB/s"
  exit 0
else
  log "❌ ERROR: Failed to create archive: $OUTPUT_FILE"
  exit 1
fi