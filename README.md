# mqtt-pg-logger

Collects MQTT messages and stores them in a Postgres database.

- Runs as Linux service.
- Provides the message payload as standard VARCHAR text and additionally converts the payload into a JSONB column if compatible.
- Clean up old messages after x days.


## Startup

### Test working MQTT broker (here Mosquitto)
```bash
sudo apt-get install mosquitto-clients

# preprare credentials
SERVER="<your server>"

# start listener
mosquitto_sub -h $SERVER -p 1883 -i "client_sub" -d -t smarthome/#

# send single message
mosquitto_pub -h $SERVER -p 1883 -i "client_pub" -d -t smarthome/test -m "test_$(date)"

# just as info: clear retained messages
mosquitto_pub -h $SERVER -p 1883 -i "client_pub" -d -t smarthome/test -n -r -d
```

### Prepare python environment
```bash
cd /opt
sudo mkdir mqtt-pg-logger
sudo chown pi:pi mqtt-pg-logger  # type in your user
git clone https://github.com/rosenloecher-it/mqtt-pg-logger mqtt-pg-logger

cd mqtt-pg-logger
virtualenv -p /usr/bin/python3 venv

# activate venv
source ./venv/bin/activate

# check python version >= 3.7
python --version

# install required packages
pip install -r requirements.txt
```

### Configuration

```bash
# cd ... goto project dir
cp ./mqtt-pg-logger.yaml.sample ./mqtt-pg-logger.yaml
```

Edit your `mqtt-pg-logger.yaml`. See comments there.

### Run

```bash
# see command line options
./mqtt-pg-logger.sh --help

# prepare your own config file based on ./mqtt-pg-logger.yaml.sample

# create database schema manually analog to ./scripts/*.sql or let the app do it
./mqtt-pg-logger.sh --create --print-logs --systemd-mode --config-file ./mqtt-pg-logger.yaml

# start the logger
./mqtt-pg-logger.sh --print-logs --systemd-mode --config-file ./mqtt-pg-logger.yaml
# abort with ctrl+c

```

## Register as systemd service
```bash
# prepare your own service script based on mqtt-pg-logger.service.sample
cp ./mqtt-pg-logger.service.sample ./mqtt-pg-logger.service

# edit/adapt pathes and user in mqtt-pg-logger.service
vi ./mqtt-pg-logger.service

# install service
sudo cp ./mqtt-pg-logger.service /etc/systemd/system/
# alternativ: sudo cp ./mqtt-pg-logger.service.sample /etc/systemd/system//mqtt-pg-logger.service
# after changes
sudo systemctl daemon-reload

# start service
sudo systemctl start mqtt-pg-logger

# check logs
journalctl -u mqtt-pg-logger
journalctl -u mqtt-pg-logger --no-pager --since "5 minutes ago"

# enable autostart at boot time
sudo systemctl enable mqtt-pg-logger.service
```

## Maintainer & License

MIT © [Raul Rosenlöcher](https://github.com/rosenloecher-it)

The code is available at [GitHub][home].

[home]: https://github.com/rosenloecher-it/mqtt-pg-logger
