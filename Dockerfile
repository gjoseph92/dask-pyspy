FROM ubuntu:20.04

RUN apt update
RUN apt install -y python3-pip
RUN pip3 install poetry

WORKDIR /opt/src/scheduler-profilers

COPY poetry.lock .
COPY pyproject.toml .

RUN poetry config virtualenvs.create false && poetry install --no-dev --no-root -E docker

COPY scheduler_profilers /opt/src/scheduler-profilers/scheduler_profilers
RUN pip3 install --no-deps .