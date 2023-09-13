FROM python:3.10.13

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . /mqtt-pg-logger/
WORKDIR /mqtt-pg-logger/src/
ENV PYTHONPATH="$PYTHONPATH:/mqtt-pg-logger/"

ENTRYPOINT ["python3"]
CMD ["mqtt_pg_logger.py", "--print-logs",  "--config-file", "/mqtt-pg-logger.yaml"]