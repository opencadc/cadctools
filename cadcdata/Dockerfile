FROM python:3.7-alpine

RUN apk --no-cache add autoconf bzip2-dev gcc git libffi-dev libmagic libxslt libxslt-dev linux-headers make musl-dev openssl-dev

WORKDIR /usr/src/app/

COPY ./ /usr/src/app/

RUN pip install .

RUN apk --no-cache del autoconf bzip2-dev gcc git libffi-dev libxslt-dev linux-headers make musl-dev openssl-dev

CMD ["cadc-data"]
