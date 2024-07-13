FROM ubuntu:24.04
WORKDIR /usr/src/app
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    git \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Need to make a venv because Ubuntu doesn't allow globally installed Python packages anymore (PEP 668)
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN python -m pip install hydrusvideodeduplicator
COPY ./docker-entrypoint.sh ./entrypoint.sh

ENV DEDUP_DATABASE_DIR=/usr/src/app/db
ENV API_URL=https://host.docker.internal:45869
CMD ["./entrypoint.sh"]
