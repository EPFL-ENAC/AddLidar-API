#!/usr/bin/env jq
. as $current | 
inputs as $previous |

# Get all paths from both scans
($current | map(.path)) as $current_paths |
($previous | map(.path)) as $previous_paths |

# Find paths in current but not in previous
$current[] | 
select(.path as $p | $previous_paths | index($p) | not) |
.path + " (New, Size: " + (.size_kb|tostring) + "KB, Files: " + (.file_count|tostring) + ", Created: " + .mod_time + ")"
