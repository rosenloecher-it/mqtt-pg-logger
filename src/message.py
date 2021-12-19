import attr
import datetime

from paho.mqtt.client import MQTTMessage


@attr.s
class Message:
    message_id = attr.ib(default=None)  # type: int

    topic = attr.ib(default=None)  # type: str
    text = attr.ib(default=None)  # type: str
    # data is extracted in database

    qos = attr.ib(default=None)  # type: int
    retain = attr.ib(default=None)  # type: bool

    time = attr.ib(default=False)  # type: datetime.datetime

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
