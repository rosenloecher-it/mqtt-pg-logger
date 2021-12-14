import unittest

import psycopg

from test.setup_test import SetupTest


class TestDb(unittest.TestCase):

    def setUp(self):
        SetupTest.init_database()

    def tearDown(self):
        SetupTest.close_database()

    def test_1(self):
        conn = psycopg.connect(**SetupTest.get_database_params())
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE hello(id int, value varchar(256))")
        cursor.execute("INSERT INTO hello values(1, 'hello'), (2, 'ciao')")
        cursor.close()
        conn.commit()
        conn.close()
