FROM debian:bookworm-slim

# Install required packages
RUN apt update && apt install -y \
    tar \
    bash \
    findutils \
    coreutils \
    curl 

# Download and install crabz from the release
RUN curl -LO https://github.com/sstadick/crabz/releases/download/v0.10.0/crabz-linux-amd64.deb \
    && dpkg -i crabz-linux-amd64.deb || apt-get -f install -y \
    && rm crabz-linux-amd64.deb

# Copy the archive script
COPY archive_folders.sh /usr/local/bin/

# Make script executable
RUN chmod +x /usr/local/bin/archive_folders.sh

# Set up working directory
WORKDIR /

ENTRYPOINT ["/usr/local/bin/archive_folders.sh"]