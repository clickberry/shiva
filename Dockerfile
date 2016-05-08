FROM node:5-onbuild
MAINTAINER Alexey Melnikov <a.melnikov@clickberry.com>

# install ffmpeg on Debian 8 (jessie)
RUN \
    echo 'deb http://www.deb-multimedia.org jessie main non-free' >> /etc/apt/sources.list && \
    echo 'deb-src http://www.deb-multimedia.org jessie main non-free' >> /etc/apt/sources.list && \
    apt-get update -y && \
    apt-get install deb-multimedia-keyring -y --force-yes && \
    apt-get update -y && \
    apt-get install -y ffmpeg

# volumes
VOLUME ["/data"]

# prepare env vars and run nodejs
RUN chmod +x ./docker-entrypoint.sh
ENTRYPOINT ["./docker-entrypoint.sh"]