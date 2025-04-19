import logging
import unittest
from unittest.async_case import IsolatedAsyncioTestCase

from sql_util import sqlite3_standard


@unittest.skip("Made to run only locally to benchmark")
class TestSQLiteOpening(IsolatedAsyncioTestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s.%(msecs)04d - %(funcName)20s() - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S', force=True)

    def test_open_huge_db(self):
        is_readonly = False
        path = r"C:\Users\Bono\hoard\hoard.contents"

        logging.debug("Opening Huge DB")
        conn = sqlite3_standard(f"file:{path}{'?mode=ro' if is_readonly else ''}", uri=True)
        logging.debug("Connected to Huge DB")
        conn.execute("SELECT COUNT(1) FROM fsobject")
        logging.debug("Executed fast query")
        conn.execute("SELECT MIN(fullpath) FROM fsobject")
        logging.debug("Executed another query")
        conn.close()
        logging.debug("Closed Huge DB")
