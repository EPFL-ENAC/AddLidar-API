FROM debian:bookworm-slim

# Install base dependencies including pigz
RUN apt-get update && apt-get install -y \
    tar \
    bash \
    findutils \
    coreutils \
    gawk \
    pigz \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy the archive script
COPY archive_folders.sh /usr/local/bin/

# Make script executable
RUN chmod +x /usr/local/bin/archive_folders.sh

# Set up working directory
WORKDIR /

ENTRYPOINT ["/usr/local/bin/archive_folders.sh"]