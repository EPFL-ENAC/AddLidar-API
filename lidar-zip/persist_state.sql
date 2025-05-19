CREATE TABLE IF NOT EXISTS folder_state (
    folder_key      TEXT PRIMARY KEY,      -- e.g. "level1/level2/level3_a"
    mission_key     TEXT NOT NULL,         -- e.g. "0003_EPFL"
    fp              TEXT NOT NULL,         -- fingerprint of *current* subtree
    output_path     TEXT NOT NULL,         -- where the archive is stored (formerly zip_path)
    size_kb         INTEGER NOT NULL,
    file_count      INTEGER NOT NULL,
    last_checked    INTEGER NOT NULL,      -- epoch (formerly last_seen)
    last_processed  INTEGER,               -- epoch, NULL = needs processing (formerly archived_at)
    processing_time INTEGER,               -- time taken for archiving in seconds
    processing_status TEXT,                -- 'success', 'failed', 'pending', NULL if never attempted
    error_message   TEXT                   -- error message if processing failed
);

CREATE TABLE IF NOT EXISTS potree_metacloud_state (
    mission_key       TEXT PRIMARY KEY,      -- e.g. "0003_EPFL"
    fp                TEXT,                  -- fingerprint of the .metacloud file (formerly metacloud_fp)
    output_path       TEXT,                  -- where potree output is stored
    last_checked      INTEGER NOT NULL,      -- epoch timestamp of last check
    last_processed    INTEGER,               -- epoch timestamp of last conversion (formerly last_converted)
    processing_time   INTEGER,               -- time taken for conversion in seconds (formerly conversion_time)
    processing_status TEXT,                  -- 'success', 'failed', 'pending', NULL if never attempted (formerly conversion_status)
    error_message     TEXT,                  -- error message if conversion failed
    FOREIGN KEY (mission_key) REFERENCES folder_state(mission_key)
);