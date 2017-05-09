FROM python:2.7

ADD . /app
WORKDIR /app
RUN cd ./cadcutils && pip install -r ./dev_requirements.txt
RUN cd ./cadcdata && pip install -r ./dev_requirements.txt
ENTRYPOINT ["./entrypoint.sh"]

CMD ["test"]