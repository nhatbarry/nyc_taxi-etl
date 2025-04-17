FROM python:3.13.3

WORKDIR /app

COPY ./requirements.txt ./requirements.txt
COPY ./ingest_data.py ./ingest_data.py
COPY ./datasets ./datasets
RUN pip install -r requirements.txt

ENTRYPOINT [ "python", "ingest_data.py" ]