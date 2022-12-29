import attr
import datetime

from paho.mqtt.client import MQTTMessage


@attr.s
class Message:
    message_id: int = attr.ib(default=None)

    topic: str = attr.ib(default=None)
    text: str = attr.ib(default=None)
    # data is extracted in database

    qos: int = attr.ib(default=None)
    retain: int = attr.ib(default=None)

    time: datetime.datetime = attr.ib(default=False)

    @classmethod
    def ensure_string(cls, value_in) -> str:
        if isinstance(value_in, bytes):
            return value_in.decode("utf-8")

        return value_in

    @classmethod
    def create(cls, mqtt_message: MQTTMessage):

        return Message(
            message_id=mqtt_message.mid,
            topic=cls.ensure_string(mqtt_message.topic),
            text=cls.ensure_string(mqtt_message.payload),
            qos=mqtt_message.qos,
            retain=mqtt_message.retain,
            # time=None  # `mqtt_message.timestamp` is not compatible with postgres
        )
