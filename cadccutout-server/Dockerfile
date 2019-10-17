FROM tiangolo/uwsgi-nginx:python3.6-alpine3.9

ENV LISTEN_PORT 8989

RUN apk --no-cache add bzip2 bzip2-dev gcc g++ git libffi-dev libxslt-dev make musl-dev \
    && pip install "git+https://github.com/at88mph/cadctools.git@fitsio-prototype#egg=cadccutout&subdirectory=cadccutout" \
    && apk --no-cache del gcc g++ git bzip2-dev libffi-dev libxslt-dev make musl-dev

COPY main.py /app/main.py

EXPOSE 8989
