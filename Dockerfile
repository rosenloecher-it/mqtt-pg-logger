FROM postgres:16.2-bookworm

ENV MQTT_BROKER 127.0.0.1
ENV POSTGRES_PASSWORD 123password

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /mqtt-pg-logger
COPY . /mqtt-pg-logger/
COPY sql/table.sql /docker-entrypoint-initdb.d/00_table.sql
COPY sql/convert.sql /docker-entrypoint-initdb.d/01_convert.sql
COPY sql/trigger.sql /docker-entrypoint-initdb.d/02_trigger.sql

COPY entrypoint.sh /
RUN chmod +x /entrypoint.sh

WORKDIR /mqtt-pg-logger
RUN python3 -m venv venv \
    && . ./venv/bin/activate \
    && pip install -r requirements.txt

EXPOSE 5432

ENTRYPOINT ["/entrypoint.sh"]

