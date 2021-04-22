FROM ubuntu:20.04

RUN apt update
RUN apt install -y python3-pip
RUN pip3 install poetry

WORKDIR /opt/src/distributed-pyspy

COPY poetry.lock .
COPY pyproject.toml .

RUN poetry config virtualenvs.create false && poetry install --no-dev --no-root -E docker

COPY distributed_pyspy /opt/src/distributed-pyspy/distributed_pyspy
RUN pip3 install --no-deps .