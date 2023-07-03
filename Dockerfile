FROM ubuntu:latest
WORKDIR /usr/src/app
RUN apt-get update
RUN apt-get install -y python3-dev python3-pip git make sqlite3 ffmpeg libavcodec-dev libavfilter-dev
RUN pip install hydrusvideodeduplicator
COPY ./start.sh ./start.sh

ENV DEDUP_DATABASE_DIR=/usr/src/app/db
CMD ["./start.sh"]