#!/bin/sh

[ -n "${MQTT_BROKER}" ] || exit 1

USER=${POSTGRES_USER:-postgres}
NAME=${POSTGRES_DB:-$USER}

cd /mqtt-pg-logger
cp mqtt-pg-logger.yaml.sample mqtt-pg-logger.yaml
chmod 600 mqtt-pg-logger.yaml
sed -i -e "s/<your[_ ]broker>/${MQTT_BROKER}/g" \
    -e "s/<your[_ ]database[_ ]password>/${POSTGRES_PASSWORD}/g" \
    -e "s/<your[_ ]database[_ ]user>/${USER}/g" \
    -e "s/<your[_ ]database[_ ]name>/${NAME}/g" \
    mqtt-pg-logger.yaml

docker-entrypoint.sh postgres &

until runuser -l postgres -c 'pg_isready -q'; do sleep 1; done

start-stop-daemon -S -x /mqtt-pg-logger/mqtt-pg-logger.sh -- --config-file /mqtt-pg-logger/mqtt-pg-logger.yaml

