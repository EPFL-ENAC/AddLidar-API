fingerprint() {
  local dir="$1"
  (cd "$dir" 2>/dev/null || { echo "Directory not found: $dir"; return 1; }
  
  if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS version - using ls instead of stat for better compatibility
    find . -type f -exec ls -laTh {} \; | awk '{print $9"|"$5"|"$6" "$7" "$8}' | sort | shasum -a 256 | cut -d\  -f1
  elif command -v stat >/dev/null 2>&1; then
    # Linux with stat available - try format that works on most systems
    find . -type f | while read -r file; do
      echo "$file|$(stat -c "%s|%Y" "$file" 2>/dev/null || echo "0|0")"
    done | sort | sha256sum | cut -d\  -f1
  else
    # Fallback for systems without proper stat support
    find . -type f | while read -r file; do
      echo "$file|$(ls -laTh "$file" | awk '{print $5"|"$6" "$7" "$8}')"
    done | sort | sha256sum | cut -d\  -f1
  fi)
}

fingerprint_data() {
  local dir="$1"
  (cd "$dir" 2>/dev/null || { echo "Directory not found: $dir"; return 1; }
  
  if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS version - using ls instead of stat for better compatibility
    find . -type f -exec ls -laTh {} \; | awk '{print $9"|"$5"|"$6" "$7" "$8}'
  elif command -v stat >/dev/null 2>&1; then
    # Linux with stat available - try format that works on most systems
    find . -type f | while read -r file; do
      echo "$file|$(stat -c "%s|%Y" "$file" 2>/dev/null || echo "0|0")"
    done
  else
    # Fallback for systems without proper stat support
    find . -type f | while read -r file; do
      echo "$file|$(ls -laTh "$file" | awk '{print $5"|"$6" "$7" "$8}')"
    done
  fi)
}

docker_fingerprint_old() {
  local dir="$1"
  local abs_path=$(cd "$dir" 2>/dev/null && pwd || { echo "Directory not found: $dir"; return 1; })
  
  docker run --rm -v "$abs_path:/data" alpine:latest /bin/sh -c '
    cd /data && 
    find . -type f -exec sh -c "echo {}\|$(stat -c %s {})|\$(stat -c %Y {})" \; | 
    sort | 
    sha256sum | 
    cut -d" " -f1
  '
}

docker_fingerprint() {
  local dir="${1:-.}"  # Default to current directory if not specified
  local abs_path
  
  # Get absolute path with error handling
  abs_path=$(cd "$dir" 2>/dev/null && pwd)
  if [ $? -ne 0 ] || [ -z "$abs_path" ]; then
    echo "Error: Directory not found or inaccessible: $dir"
    return 1
  fi
  
  # For Windows users running Docker Desktop (adjust path format if needed)
  if [[ "$OSTYPE" == "msys"* ]] || [[ "$OSTYPE" == "cygwin"* ]]; then
    abs_path=$(echo "$abs_path" | sed 's|^/\([a-z]\)/|\1:/|i' | sed 's|/|\\|g')
  fi
  
  # Run the fingerprint command in Docker
  docker run --rm -v "${abs_path}:/data:ro" alpine:latest /bin/sh -c '
    cd /data && 
    find . -type f -print0 | sort -z | xargs -0 stat -c "%n|%s|%Y" | 
    sha256sum | 
    cut -d" " -f1
  '
}