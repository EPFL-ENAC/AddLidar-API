#!/bin/bash
 # Optimized script to create compressed archives for second-level folders based on benchmark results

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

SOURCE_DIR="$1"
OUTPUT_DIR="$2"

# Fixed thread count for pigz
COMPRESS_THREADS=8

log "Source directory: $SOURCE_DIR"
log "Output directory: $OUTPUT_DIR"
log "Using fixed $COMPRESS_THREADS compression threads"

mkdir -p "$OUTPUT_DIR"
log "Created output directory structure"

# Find all second-level directories first
second_level_dirs=()
while IFS= read -r first_dir; do
  first_name=$(basename "$first_dir")
  
  # Create directory for first-level folder
  first_level_output_dir="$OUTPUT_DIR/$first_name"
  mkdir -p "$first_level_output_dir"
  
  while IFS= read -r second_dir; do
    second_level_dirs+=("$second_dir|$first_level_output_dir")
  done < <(find "$first_dir" -mindepth 1 -maxdepth 1 -type d)
done < <(find "$SOURCE_DIR" -mindepth 1 -maxdepth 1 -type d)

total_dirs=${#second_level_dirs[@]}
log "Found $total_dirs second-level directories to process"

# Process directories
for ((i=0; i<${#second_level_dirs[@]}; i++)); do
  IFS='|' read -r second_dir first_level_output_dir <<< "${second_level_dirs[$i]}"
  second_name=$(basename "$second_dir")
  archive_path="$first_level_output_dir/${second_name}.tar.gz"
  
  # Skip if archive already exists
  if [ -f "$archive_path" ]; then
    log "Skipping existing archive: $archive_path ($(($i+1))/$total_dirs)"
    continue
  fi
  
  log "Creating archive for: $second_name ($(($i+1))/$total_dirs)"

  # Get original size
  original_size=$(du -sb "$second_dir" | cut -f1)
  original_size_human=$(format_size $original_size)
  log "Original size: $original_size_human"

  # Create archive with timing
  start_time=$(date +%s)
  
  log "Creating tar and compressing with pigz..."
  
  # Use direct tar-to-pigz approach with fixed thread count
  tar -C "$(dirname "$second_dir")" -cf - "$(basename "$second_dir")" | \
  pigz -p $COMPRESS_THREADS > "$archive_path"
  compression_status=$?
  
  end_time=$(date +%s)
  elapsed=$((end_time - start_time))
  
  if [ -f "$archive_path" ] && [ $compression_status -eq 0 ]; then
    compressed_size=$(stat -c%s "$archive_path")
    compressed_size_human=$(format_size $compressed_size)
    
    # Calculate compression ratio and speed
    ratio=$(awk "BEGIN {printf \"%.2f\", $compressed_size * 100 / $original_size}")
    speed=$(awk "BEGIN {printf \"%.2f\", $original_size / 1048576 / $elapsed}")
    
    log "✅ Archive created: $archive_path"
    log "   Time taken: ${elapsed}s"
    log "   Original size: $original_size_human"
    log "   Compressed size: $compressed_size_human (${ratio}% of original)"
    log "   Compression speed: ${speed} MB/s"
  else
    log "❌ ERROR: Failed to create archive: $archive_path"
  fi
done

log "Script completed"