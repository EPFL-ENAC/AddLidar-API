#!/bin/bash
# Minimal script to create compressed archives for all second-level folders

# Setup logging
log() {
  local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
  echo "[$timestamp] $1"
}

format_size() {
  # Convert bytes to human-readable format without bc
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

calculate_percentage() {
  # Calculate percentage without bc
  awk "BEGIN {printf \"%.2f\", $1 * 100 / $2}"
}

calculate_speed() {
  # Calculate speed in MB/s without bc
  awk "BEGIN {printf \"%.2f\", $1 / 1048576 / $2}"
}

monitor_progress() {
  local pid=$1
  local original_size=$2
  local file_path=$3
  local start_time=$4
  local prev_size=0
  local prev_time=$(date +%s)
  
  # Check every 2 seconds
  while kill -0 $pid 2>/dev/null; do
    if [ -f "$file_path" ]; then
      current_size=$(stat -c%s "$file_path" 2>/dev/null || echo 0)
      current_time=$(date +%s)
      time_diff=$((current_time - prev_time))
      
      if [ "$current_size" -gt 0 ] && [ "$original_size" -gt 0 ] && [ "$time_diff" -gt 0 ]; then
        # Calculate progress percentage
        progress=$(calculate_percentage $current_size $original_size)
        current_size_human=$(format_size $current_size)
        
        # Calculate current speed
        size_diff=$((current_size - prev_size))
        if [ "$size_diff" -gt 0 ]; then
          current_speed=$(calculate_speed $size_diff $time_diff)
          
          # Calculate ETA
          remaining_bytes=$((original_size - current_size))
          if [ "$size_diff" -gt 0 ]; then
            eta_seconds=$(awk "BEGIN {printf \"%.0f\", $remaining_bytes / ($size_diff / $time_diff)}")
            eta_human=$(date -d@$eta_seconds -u +%H:%M:%S)
            log "   Progress: $current_size_human / $progress% complete - Speed: ${current_speed} MB/s - ETA: ${eta_human}"
          else
            log "   Progress: $current_size_human / $progress% complete"
          fi
        else
          log "   Progress: $current_size_human / $progress% complete"
        fi
        
        # Update previous values for next iteration
        prev_size=$current_size
        prev_time=$current_time
      fi
    fi
    sleep 2
  done
}

log "Script started"

SOURCE_DIR="$1"
OUTPUT_DIR="$2"

log "Source directory: $SOURCE_DIR"
log "Output directory: $OUTPUT_DIR"

mkdir -p "$OUTPUT_DIR"
log "Created output directory structure"

find "$SOURCE_DIR" -mindepth 1 -maxdepth 1 -type d | while read -r first_dir; do
  first_name=$(basename "$first_dir")
  log "Processing first-level folder: $first_name"
  
  # Create directory for first-level folder
  first_level_output_dir="$OUTPUT_DIR/$first_name"
  mkdir -p "$first_level_output_dir"
  
  find "$first_dir" -mindepth 1 -maxdepth 1 -type d | while read -r second_dir; do
    second_name=$(basename "$second_dir")
    archive_path="$first_level_output_dir/${second_name}.tar.gz"
    
    # Skip if archive already exists
    if [ -f "$archive_path" ]; then
      log "Skipping existing archive: $archive_path"
      continue
    fi
    
    log "Creating archive for: $second_name"

    # Get original size
    original_size=$(du -sb "$second_dir" | cut -f1)
    original_size_human=$(format_size $original_size)
    log "Original size: $original_size_human"

    # Create archive with timing
    start_time=$(date +%s)
    
    log "Creating tar and compressing with pigz..."
    # Use pv for progress monitoring if available
    if command -v pv >/dev/null 2>&1; then
      # Use pv to show progress
      tar -cf - -C "$(dirname "$second_dir")" "$(basename "$second_dir")" 2>/dev/null | 
        pv -s "$original_size" | 
        pigz -1 -p 4 > "$archive_path"
      compression_status=$?
    else
      # Start compression in background
      tar -cf - -C "$(dirname "$second_dir")" "$(basename "$second_dir")" 2>/dev/null | 
        pigz -1 -p 4 > "$archive_path" &
      pigz_pid=$!
      
      # Monitor progress
      monitor_progress $pigz_pid $original_size "$archive_path"
      
      # Wait for completion
      wait $pigz_pid
      compression_status=$?
    fi
    
    end_time=$(date +%s)
    elapsed=$((end_time - start_time))
    
    if [ -f "$archive_path" ] && [ $compression_status -eq 0 ]; then
      compressed_size=$(stat -c%s "$archive_path")
      compressed_size_human=$(format_size $compressed_size)
      
      # Calculate compression ratio and speed
      ratio=$(calculate_percentage $compressed_size $original_size)
      speed=$(calculate_speed $original_size $elapsed)
      
      log "✅ Archive created: $archive_path"
      log "   Time taken: ${elapsed}s"
      log "   Original size: $original_size_human"
      log "   Compressed size: $compressed_size_human (${ratio}% of original)"
      log "   Compression speed: ${speed} MB/s"
    else
      log "❌ ERROR: Failed to create archive: $archive_path"
    fi
  done
done

log "Script completed"