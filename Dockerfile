FROM ubuntu:latest
WORKDIR /usr/src/app
RUN apt-get update && apt-get install -y \
    python3.11 \
    python3-pip \
    git \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*
RUN python3.11 -m pip install hydrusvideodeduplicator
COPY ./docker-entrypoint.sh ./entrypoint.sh

ENV DEDUP_DATABASE_DIR=/usr/src/app/db
ENV API_URL=https://host.docker.internal:45869
CMD ["./entrypoint.sh"]
