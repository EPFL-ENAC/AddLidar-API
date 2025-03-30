#!/bin/bash
# Minimal script to create compressed archives for all second-level folders
SOURCE_DIR="$1"
OUTPUT_DIR="$2"
mkdir -p "$OUTPUT_DIR"

find "$SOURCE_DIR" -mindepth 1 -maxdepth 1 -type d | while read -r first_dir; do
  first_name=$(basename "$first_dir")
  
  # Create directory for first-level folder
  first_level_output_dir="$OUTPUT_DIR/$first_name"
  mkdir -p "$first_level_output_dir"
  
  find "$first_dir" -mindepth 1 -maxdepth 1 -type d | while read -r second_dir; do
    second_name=$(basename "$second_dir")
    archive_path="$first_level_output_dir/${second_name}.tar.gz"
    
    # Skip if archive already exists
    [ -f "$archive_path" ] && continue
    
    # Create archive
    temp_tar=$(mktemp)
    tar -cf "$temp_tar" -C "$(dirname "$second_dir")" "$(basename "$second_dir")" && \
    crabz -l 1 -p 4 "$temp_tar" -o "$archive_path"
    rm -f "$temp_tar"
  done
done