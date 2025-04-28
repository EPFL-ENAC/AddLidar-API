#!/bin/bash
sqlite3 state/archive.db ".mode json" "
SELECT
  folder_key                     AS folder_path,
  size_kb                        AS folder_size_kb,
  file_count                     AS folder_file_count,
  zip_path                       AS archive_path,
  archived_at                    AS archive_mod_time_epoch,
  datetime(archived_at,'unixepoch') AS archive_mod_time,
  last_seen                      AS folder_mod_time_epoch,
  datetime(last_seen,'unixepoch')   AS folder_mod_time
FROM   folder_state
ORDER  BY last_seen DESC;
"  > state.json


