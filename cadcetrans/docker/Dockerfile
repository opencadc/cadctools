FROM debian

RUN apt-get -y update && apt-get -y install fitsverify python3-pip

RUN mkdir -p /root/.config/cadc
COPY cadc-etrans-config namecheck.xml /root/.config/cadc/
COPY entrypoint.sh .

RUN pip3 install cadcetrans

RUN mkdir /logging

ENTRYPOINT ["./entrypoint.sh"]
CMD []
