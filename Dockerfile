FROM ubuntu:20.04

RUN apt update
RUN apt install -y python3-pip
RUN pip3 install --upgrade pip poetry
# ^ pip: https://github.com/python-poetry/poetry/issues/5104

WORKDIR /opt/src/scheduler-profilers

COPY poetry.lock .
COPY pyproject.toml .

RUN poetry config virtualenvs.create false && poetry install --only main,docker

COPY scheduler_profilers /opt/src/scheduler-profilers/scheduler_profilers
RUN pip3 install --no-deps .