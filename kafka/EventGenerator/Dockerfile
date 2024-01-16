FROM python:3.10.12-slim-buster

LABEL maintainer="Green Orchid"
LABEL product="POC - Event Generator"

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
COPY . /app

CMD ["python", "./main.py"]
