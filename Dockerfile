FROM python:2.7

ADD . /app
WORKDIR /app
RUN cd ./cadcutils && pip install -e ".[test]"
RUN cd ./cadcdata && pip install -e ".[test]"
ENTRYPOINT ["./entrypoint.sh"]

CMD ["test"]
