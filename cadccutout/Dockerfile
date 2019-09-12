ARG PYTHON_VERSION=3.7
ARG ENV='PROD'
FROM opencadc/astroquery:${PYTHON_VERSION}-alpine

# Used for memory_profiler
RUN apk --no-cache add linux-headers make autoconf musl-dev gcc git bzip2-dev

COPY ./ ./

RUN pip install .

CMD [ "cadccutout" ]
