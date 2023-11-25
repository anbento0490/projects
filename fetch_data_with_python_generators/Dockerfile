FROM jupyter/scipy-notebook:latest

USER root

RUN apt-get update && apt-get install

RUN apt-get install -y \
  dos2unix \
  libpq-dev \
  libmariadb-dev-compat \
  libmariadb-dev \
  gcc \
  && apt-get clean

RUN pip3 install pandas psycopg2 boto3 awswrangler minio