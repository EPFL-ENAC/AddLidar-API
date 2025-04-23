#!/bin/bash
# LiDAR Directory Change Monitor
# Tracks changes in LiDAR directories up to the second level and identifies folders that need processing

# Configuration
LIDAR_ROOT="./lidar"                  # Root LiDAR directory to monitor
DATA_DIR="./lidar-monitor"    # Directory to store tracking data
CURRENT_SCAN="${DATA_DIR}/current.json"  # Current scan results
PREVIOUS_SCAN="${DATA_DIR}/previous.json" # Previous scan results
CHANGES_FILE="${DATA_DIR}/changes.txt"    # File to record detected changes
LOG_FILE="${DATA_DIR}/monitor.log"        # Log file

# Ensure data directory exists
mkdir -p "${DATA_DIR}"

# Log function
log() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "${LOG_FILE}"
}

log "Starting LiDAR directory change monitor"

# Move current scan to previous if it exists
if [ -f "${CURRENT_SCAN}" ]; then
    mv "${CURRENT_SCAN}" "${PREVIOUS_SCAN}"
    log "Saved previous scan data"
fi

# Start the current scan
log "Beginning new scan of ${LIDAR_ROOT}"

# Process each directory up to the second level
# Output format: {"path": "/path/to/dir", "size_kb": 1234, "mod_time": "2025-04-21 10:00:00", "file_count": 42}
(
echo "["
first=true

# Find all directories up to the second level (0=root, 1=first level, 2=second level)
find -L "${LIDAR_ROOT}" -mindepth 1 -maxdepth 2 -type d | sort | while read -r dir; do
    # Calculate metrics in parallel for better performance
    # Skip the separator for the first entry
    if [ "$first" = true ]; then
        first=false
    else
        echo ","
    fi
    
    # Get directory metrics (using multiple processes for speed)
    size_kb=$(du -sk "${dir}" 2>/dev/null | cut -f1)
    mod_time=$(stat -c %Y "${dir}" 2>/dev/null)
    mod_time_human=$(date -d "@${mod_time}" +"%Y-%m-%d %H:%M:%S" 2>/dev/null)
    file_count=$(find -L "${dir}" -type f -printf . 2>/dev/null | wc -c)
    
    # Output as JSON
    echo "  {"
    echo "    \"path\": \"${dir}\","
    echo "    \"size_kb\": ${size_kb},"
    echo "    \"mod_time\": \"${mod_time_human}\","
    echo "    \"mod_time_epoch\": ${mod_time},"
    echo "    \"file_count\": ${file_count}"
    echo -n "  }"
done
echo
echo "]"
) > "${CURRENT_SCAN}"

log "Scan completed"

# Compare with previous scan to detect changes
if [ -f "${PREVIOUS_SCAN}" ]; then
    log "Comparing with previous scan to detect changes"
    
    # Use jq to compare the two JSON files and identify changes
    # This requires jq to be installed
    if command -v jq >/dev/null 2>&1; then
        # Process with jq
        (
        echo "Detected changes ($(date)):"
        echo "====================================="
        echo "Folders that need processing:"
        echo
        
        # Compare current with previous using jq
        # Logic: For each directory in current scan, find matching directory in previous scan
        # If size, mod_time, or file_count changed, report it
        jq -r -f ./diff-changes.jq "${CURRENT_SCAN}" "${PREVIOUS_SCAN}" | sort
        
        # Also check for new directories that weren't in the previous scan
        echo
        echo "New directories:"
        jq -r -f ./new-directories.jq "${CURRENT_SCAN}" "${PREVIOUS_SCAN}" | sort
        
        echo
        echo "====================================="
        ) > "${CHANGES_FILE}"
        
        log "Change detection completed. Check ${CHANGES_FILE} for details."
        cat "${CHANGES_FILE}"
    else
        log "ERROR: jq is not installed. Cannot compare scans."
        echo "Please install jq with: sudo apt-get install jq"
    fi
else
    log "No previous scan found. This is the first run."
fi

log "Monitor completed successfully"
