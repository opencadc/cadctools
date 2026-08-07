FROM python:3.10

ADD . /app
WORKDIR /app
RUN cd ./cadcutils && pip install -e ".[test]"
RUN cd ./cadcdata && pip install -e ".[test]"
ENTRYPOINT ["./entrypoint.sh"]

CMD ["test"]
