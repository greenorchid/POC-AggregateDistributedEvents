FROM python:3.10.12-slim-buster

LABEL maintainer="Green Orchid"
LABEL product="POC - Event Aggregator"

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
COPY . /app

ENV FAUST_WEB_PORT=6066

CMD ["sh", "-c", "faust -A main worker -l info --web-port=$FAUST_WEB_PORT"]
