FROM python:3.7-slim

RUN apt-get update && \
    apt-get install -y gcc make libc-dev libffi-dev libssl-dev libxslt-dev libcurl4-openssl-dev git fitsverify libcfitsio-dev libcfitsio7 libbz2-dev

RUN pip install --upgrade pip && \
    pip install cadcdata fitsio astropy pep8 flake8 pytest pylint autopep8

RUN apt-get clean && \
    apt-get autoclean