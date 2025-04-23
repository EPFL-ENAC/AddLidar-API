CREATE TABLE IF NOT EXISTS folder_state (
    folder_key  TEXT PRIMARY KEY,      -- e.g. "level1/level2_a"
    zip_path    TEXT NOT NULL,
    fp          TEXT NOT NULL,         -- fingerprint of *current* subtree
    size_kb     INTEGER NOT NULL,
    file_count  INTEGER NOT NULL,
    last_seen   INTEGER NOT NULL,      -- epoch
    archived_at INTEGER                -- epoch, NULL = needs archive
);
