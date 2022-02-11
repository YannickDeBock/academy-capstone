FROM python:3.8-alpine3.14
WORKDIR /ingest
COPY requirements.txt /ingest
USER root
RUN python3 -m pip install -r requirements.txt
COPY ingest.py /ingest
CMD python3 ingest.py