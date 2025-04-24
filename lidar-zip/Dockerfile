FROM debian:bookworm-slim

# Install base dependencies including pigz
RUN apt-get update && apt-get install -y \
    tar \
    bash \
    findutils \
    coreutils \
    sqlite3 \
    gawk \
    pigz \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy the archive script
COPY archive_one_folder.sh /usr/local/bin/

# Make script executable
RUN chmod +x /usr/local/bin/archive_one_folder.sh

# Set up working directory
WORKDIR /
