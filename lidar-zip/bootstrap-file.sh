#!/bin/bash
# Bootstrap script to generate JSON mapping for tar.gz files.
#
# For each level 2 directory found in LIDAR_ROOT, the corresponding tar.gz is assumed
# to be located in LIDAR_ZIPS with the same relative path plus a ".tar.gz" extension.
#
# JSON format example:
# {
#   "path": "/0001_Mission_Root/00_Raw_Lidar_SDC_Data.tar.gz",
#   "size_kb": 8,
#   "mod_time": "2025-04-06 15:42:39",
#   "mod_time_epoch": 1743946959,
#   "file_count": 1
# }
#
# Usage: ./bootstrap-file.sh

# Configuration
LIDAR_ROOT="./lidar"          # Folder where level 2 directories are found
LIDAR_ZIPS="./lidar-zips"      # Base folder where tar.gz files are stored
OUTPUT_JSON="./lidar-monitor/bootstrap.json"

# Ensure output directory exists
mkdir -p "$(dirname "${OUTPUT_JSON}")"

# Begin JSON output
echo "[" > "${OUTPUT_JSON}"
first=true

# Iterate over level 2 directories in LIDAR_ROOT
while IFS= read -r folder; do
    # Compute relative path using Python instead of realpath's --relative-to option
    rel_path=$(python3 -c "import os,sys; print(os.path.relpath(os.path.realpath(sys.argv[1]), os.path.realpath(sys.argv[2])))" "${folder}" "${LIDAR_ROOT}")
    # Build the tarball path in LIDAR_ZIPS and the JSON path starting with a leading '/'
    tarball="${LIDAR_ZIPS}/${rel_path}.tar.gz"
    json_path="/${rel_path}.tar.gz"
    
    if [ "${first}" = true ]; then
        first=false
    else
        echo "," >> "${OUTPUT_JSON}"
    fi

    # If tarball exists, get file metrics; otherwise, all metrics are null
    if [ -f "${tarball}" ]; then
        size_kb=$(du -sk "${tarball}" 2>/dev/null | cut -f1)
        # macOS: use 'stat -f %m' and 'date -r'
        mod_time_epoch=$(stat -f %m "${tarball}" 2>/dev/null)
        mod_time=$(date -r "${mod_time_epoch}" +"%Y-%m-%d %H:%M:%S" 2>/dev/null)
        file_count=$(tar -tzf "${tarball}" 2>/dev/null | wc -l)
    else
        size_kb=null
        mod_time="null"
        mod_time_epoch=null
        file_count=null
    fi

    # Write JSON object, ensuring proper JSON number/null formatting.
    printf '  {"path": "%s", "size_kb": %s, "mod_time": "%s", "mod_time_epoch": %s, "file_count": %s}' \
        "${json_path}" "${size_kb}" "${mod_time}" "${mod_time_epoch}" "${file_count}" >> "${OUTPUT_JSON}"
done < <(find -L "${LIDAR_ROOT}" -mindepth 2 -maxdepth 2 -type d | sort)

echo "" >> "${OUTPUT_JSON}"
echo "]" >> "${OUTPUT_JSON}"

echo "Bootstrap JSON generated at ${OUTPUT_JSON}"