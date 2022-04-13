import datetime
import unittest

from tzlocal import get_localzone

from src.database import DatabaseConfKey
from src.message_store import MessageStore
from src.message import Message
from test.setup_test import SetupTest


class TestMessageStore(unittest.TestCase):

    CONFIG_CLEAN_UP_AFTER_DAYS = 21

    def setUp(self):
        SetupTest.init_database()
        # SetupTest.init_logging()

        SetupTest.execute_commands(["delete from journal"])

        self.config_clean_up_after_days = 21

        database_params = SetupTest.get_database_params()
        database_params[DatabaseConfKey.CLEAN_UP_AFTER_DAYS] = self.CONFIG_CLEAN_UP_AFTER_DAYS

        self.database = MessageStore(database_params)
        self.database.connect()

    def tearDown(self):
        if self.database:
            self.database.close()
        self.database = None

        SetupTest.close_database()

    def test_insert(self):
        insert_count = 10

        def generate_message(i):
            return Message(
                message_id=i,
                topic=f"topic-{i + 10000}",
                text=f"text-{i + 100000}",
                qos=(i % 2 + 1),
                retain=(i % 2),
                time=datetime.datetime(2020, 2, 2, (i % 10 + 1), 0, 0, tzinfo=get_localzone()),
            )

        messages = [generate_message(i) for i in range(1, 1 + insert_count)]

        self.database.store(messages)

        rows = SetupTest.query_all("select * from journal")
        self.assertEqual(len(rows), insert_count)
        for row in rows:
            self.assertGreaterEqual(row.pop("journal_id"), 0)
            row.pop("data")

            message_id = row["message_id"]
            compare = generate_message(message_id)
            current = Message(**row)
            self.assertEqual(current, compare)

    def test_cleanup(self):
        self.assertEqual(self.database._clean_up_after_days, self.CONFIG_CLEAN_UP_AFTER_DAYS)

        time_now = datetime.datetime.now(tz=get_localzone())
        time_remain = time_now - datetime.timedelta(days=self.database._clean_up_after_days - 1)
        time_remove = time_now - datetime.timedelta(days=self.database._clean_up_after_days + 1)

        self.database.now = time_now

        def generate_message(i, time):
            return Message(
                message_id=i,
                topic=f"topic-{i + 10000}",
                text=f"text-{i + 100000}",
                qos=(i % 2 + 1),
                retain=(i % 2),
                time=time,
            )

        messages = [generate_message(i, time_remove) for i in range(1, 11)]
        self.database.store(messages)
        messages = [generate_message(i + 1000, time_remain) for i in range(1, 11)]
        self.database.store(messages)

        fetched = SetupTest.query_one("select count(1) from journal")
        self.assertEqual(fetched["count"], 20)

        self.database.clean_up()
        fetched = SetupTest.query_one("select count(1) from journal")
        self.assertEqual(fetched["count"], 10)

    def test_trigger_valid_json(self):
        message1 = Message(
            message_id=1, topic="topic1", qos=1, retain=0,
            text='{"text": "text", "int": 9 }',
            time=datetime.datetime(2020, 2, 2, 9, 0, 0, tzinfo=get_localzone()),
        )
        self.database.store([message1])

        fetched = SetupTest.query_one("select count(1) from journal")
        self.assertEqual(fetched["count"], 1)

        # valid json
        row = SetupTest.query_all("select * from journal where message_id=1")[0]
        self.assertGreaterEqual(row.pop("journal_id"), 0)
        json_data = row.pop("data")

        reloaded_message = Message(**row)
        self.assertEqual(reloaded_message, message1)

        self.assertTrue(json_data is not None)

    def test_trigger_invalid_json(self):
        message1 = Message(
            message_id=1, topic="topic1", qos=1, retain=0,
            text='text',  # is not converted to JSON
            time=datetime.datetime(2020, 2, 2, 1, 0, 0, tzinfo=get_localzone()),
        )
        message2 = Message(
            message_id=2, topic="topic2", qos=1, retain=0,
            text='{"text": "text", "int": 9 ',  # invalid JSON
            time=datetime.datetime(2020, 2, 2, 2, 0, 0, tzinfo=get_localzone()),
        )
        message3 = Message(
            message_id=3, topic="topic3", qos=1, retain=0,
            text='123',  # would normally be converted to JSON, but is suppressed by the convert function
            time=datetime.datetime(2020, 2, 2, 3, 0, 0, tzinfo=get_localzone()),
        )
        self.database.store([message1, message2, message3])

        fetched = SetupTest.query_one("select count(1) from journal")
        self.assertEqual(fetched["count"], 3)

        def check_message(message_id, compare_message):
            row = SetupTest.query_all(f"select * from journal where message_id={message_id}")[0]
            self.assertGreaterEqual(row.pop("journal_id"), 0)
            json_data = row.pop("data")
            self.assertTrue(json_data is None)

            reloaded_message = Message(**row)
            self.assertEqual(reloaded_message, compare_message)

        check_message(1, message1)
        check_message(2, message2)
        check_message(3, message3)
