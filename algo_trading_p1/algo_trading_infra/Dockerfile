# Set Airflow and Python version
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12

# Base image
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root

RUN apt-get update && apt-get install

RUN apt-get install -y \
  dos2unix \
  libpq-dev \
  libmariadb-dev-compat \
  libmariadb-dev \
  gcc \
  && apt-get clean

USER airflow

COPY requirements.txt .

RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt