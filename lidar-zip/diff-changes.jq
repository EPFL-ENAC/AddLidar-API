#!/usr/bin/env jq
# jq script to compare current and previous scans
# This script compares two JSON files representing directory scans
. as $current | 
inputs as $previous |
$current[] | 
. as $curr | 
$previous[] | 
select(.path == $curr.path) as $prev |
if ($curr.size_kb != $prev.size_kb or 
    $curr.mod_time_epoch > $prev.mod_time_epoch or 
    $curr.file_count != $prev.file_count) then
    $curr.path + " (Size: " + 
    (if $curr.size_kb != $prev.size_kb then 
        "changed from " + ($prev.size_kb|tostring) + "KB to " + ($curr.size_kb|tostring) + "KB" 
    else 
        "unchanged" 
    end) + 
    ", Files: " + 
    (if $curr.file_count != $prev.file_count then 
        "changed from " + ($prev.file_count|tostring) + " to " + ($curr.file_count|tostring) 
    else 
        "unchanged" 
    end) + 
    ", Last Modified: " + $curr.mod_time + ")"
else
    empty
end