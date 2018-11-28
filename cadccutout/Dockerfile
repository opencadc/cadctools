ARG PYTHON_VERSION=3.7
FROM opencadc/astroquery:${PYTHON_VERSION}-alpine

COPY ./ ./

RUN pip install .

CMD [ "cadccutout" ]
