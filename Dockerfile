FROM ubuntu:20.04

RUN apt update
RUN apt install -y python3-pip
RUN pip3 install --upgrade pip poetry
# ^ pip: https://github.com/python-poetry/poetry/issues/5104

WORKDIR /opt/src/dask-profiling

COPY poetry.lock .
COPY pyproject.toml .

RUN poetry config virtualenvs.in-project true && \
    poetry install --only main,docker

ENV PATH="/opt/src/dask-profiling/.venv/bin:$PATH"

COPY dask_profiling /opt/src/dask-profiling/dask_profiling
RUN pip3 install --no-deps .