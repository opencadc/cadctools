ARG PYTHON_VERSION=3.7
ARG ENV='PROD'
FROM opencadc/astropy:${PYTHON_VERSION}-alpine

# Used for memory_profiler
RUN apk --no-cache add autoconf bzip2-dev gcc git linux-headers make musl-dev

COPY ./ ./

RUN pip install .

CMD [ "cadccutout" ]
