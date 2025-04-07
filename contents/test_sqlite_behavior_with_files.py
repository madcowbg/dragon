import unittest

from contents.hoard import init_hoard_db_tables
from sql_util import sqlite3_standard


class TestSQLITEBehavior(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3_standard(":memory:")

        init_hoard_db_tables(self.conn)
        self.conn.commit()

    def tearDown(self):
        self.assertEqual(
            [],
            list(self.conn.execute(
                "select * from folder_structure "
                "where fullpath not in (select fullpath from fsobject) AND fsobject_id IS NOT NULL")))

        self.assertEqual(
            [],
            list(self.conn.execute(
                "select * from fsobject where fullpath not in (select fullpath from folder_structure)")))

        self.conn.close()

    def test_adding_files(self):
        self.conn.execute(
            "INSERT INTO fsobject (fullpath, isdir) VALUES ('/wat/me/test.me.different', FALSE)")

        self.assertEqual([
            (1, '/wat/me/test.me.different', 0, None, None, None)],
            list(self.conn.execute("SELECT * FROM fsobject ORDER BY fullpath")))
        self.assertEqual([
            ('', None, 1, None),
            ('/wat', None, 1, ''),
            ('/wat/me', None, 1, '/wat'),
            ('/wat/me/test.me.different', 1, 0, '/wat/me')],
            list(self.conn.execute("SELECT * FROM folder_structure ORDER BY fullpath")))

        self.conn.execute(
            "INSERT INTO fsobject (fullpath, isdir) VALUES ('/wat', TRUE)")

        self.assertEqual([
            (2, '/wat', 1, None, None, None),
            (1, '/wat/me/test.me.different', 0, None, None, None)],
            list(self.conn.execute("SELECT * FROM fsobject ORDER BY fullpath")))
        self.assertEqual([
            ('', None, 1, None),
            ('/wat', 2, 1, ''),
            ('/wat/me', None, 1, '/wat'),
            ('/wat/me/test.me.different', 1, 0, '/wat/me')],
            list(self.conn.execute("SELECT * FROM folder_structure ORDER BY fullpath")))

    def test_adding_files_different_order(self):
        self.conn.execute(
            "INSERT INTO fsobject (fullpath, isdir) VALUES ('/wat', TRUE)")
        self.conn.execute(
            "INSERT INTO fsobject (fullpath, isdir) VALUES ('/wat/me/test.me.different', FALSE)")

        self.assertEqual([
            (1, '/wat', 1, None, None, None),
            (2, '/wat/me/test.me.different', 0, None, None, None)],
            list(self.conn.execute("SELECT * FROM fsobject ORDER BY fullpath")))
        self.assertEqual([
            ('', None, 1, None),
            ('/wat', 1, 1, ''),
            ('/wat/me', None, 1, '/wat'),
            ('/wat/me/test.me.different', 2, 0, '/wat/me')],
            list(self.conn.execute("SELECT * FROM folder_structure ORDER BY fullpath")))

    def test_deleting_files(self):
        self.conn.execute(
            "INSERT INTO fsobject (fullpath, isdir) VALUES ('/wat/me/test.me.different', FALSE)")
        self.conn.execute(
            "INSERT INTO fsobject (fullpath, isdir) VALUES ('/wat', TRUE)")

        self.conn.execute(
            "DELETE FROM fsobject WHERE fullpath = '/wat'")

        self.assertEqual([
            (1, '/wat/me/test.me.different', 0, None, None, None)],
            list(self.conn.execute("SELECT * FROM fsobject ORDER BY fullpath")))
        self.assertEqual([
            ('', None, 1, None),
            ('/wat', None, 1, ''),
            ('/wat/me', None, 1, '/wat'),
            ('/wat/me/test.me.different', 1, 0, '/wat/me')],
            list(self.conn.execute("SELECT * FROM folder_structure ORDER BY fullpath")))

        self.conn.execute(
            "INSERT INTO fsobject (fullpath, isdir) VALUES ('/wat', TRUE)")
        self.assertEqual([
            ('', None, 1, None),
            ('/wat', 3, 1, ''),
            ('/wat/me', None, 1, '/wat'),
            ('/wat/me/test.me.different', 1, 0, '/wat/me')],
            list(self.conn.execute("SELECT * FROM folder_structure ORDER BY fullpath")))

        self.conn.execute(
            "DELETE FROM fsobject WHERE fullpath = '/wat/me/test.me.different'")
        self.assertEqual([
            (3, '/wat', 1, None, None, None)],
            list(self.conn.execute("SELECT * FROM fsobject ORDER BY fullpath")))
        self.assertEqual([
            ('', None, 1, None),
            ('/wat', 3, 1, '')],
            list(self.conn.execute("SELECT * FROM folder_structure ORDER BY fullpath")))

    def test_deleting_not_in_order(self):
        self.conn.executescript("""
            INSERT into fsobject (fullpath, isdir) 
            VALUES ('/somebody.twat2', TRUE);
            
            insert into fsobject (fullpath, isdir)
            VALUES ('/some/other/thing/is_in', TRUE);
            
            insert into fsobject (fullpath, isdir)
            VALUES ('/some/other/thing/is_in/here.java', FALSE);
            
            insert into fsobject (fullpath, isdir)
            VALUES ('/some/other/thing/is_in/here.too.java', FALSE);
            
            insert into fsobject (fullpath, isdir)
            VALUES ('/some/other/thing/is_folder', TRUE);
            
            insert into fsobject (fullpath, isdir)
            VALUES ('/some/other/thing_another/is_folder', TRUE);
            """)

        self.assertEqual([
            (5, '/some/other/thing/is_folder', 1, None, None, None),
            (2, '/some/other/thing/is_in', 1, None, None, None),
            (3, '/some/other/thing/is_in/here.java', 0, None, None, None),
            (4, '/some/other/thing/is_in/here.too.java', 0, None, None, None),
            (6, '/some/other/thing_another/is_folder', 1, None, None, None),
            (1, '/somebody.twat2', 1, None, None, None)],
            list(self.conn.execute("SELECT * FROM fsobject ORDER BY fullpath")))
        self.assertEqual([
            ('', None, 1, None),
            ('/some', None, 1, ''),
            ('/some/other', None, 1, '/some'),
            ('/some/other/thing', None, 1, '/some/other'),
            ('/some/other/thing/is_folder', 5, 1, '/some/other/thing'),
            ('/some/other/thing/is_in', 2, 1, '/some/other/thing'),
            ('/some/other/thing/is_in/here.java', 3, 0, '/some/other/thing/is_in'),
            ('/some/other/thing/is_in/here.too.java', 4, 0, '/some/other/thing/is_in'),
            ('/some/other/thing_another', None, 1, '/some/other'),
            ('/some/other/thing_another/is_folder', 6, 1, '/some/other/thing_another'),
            ('/somebody.twat2', 1, 1, '')],
            list(self.conn.execute("SELECT * FROM folder_structure ORDER BY fullpath")))

        self.conn.execute(
            "DELETE FROM fsobject WHERE fullpath = '/some/other/thing/is_in'")

        self.assertEqual([
            (5, '/some/other/thing/is_folder', 1, None, None, None),
            (3, '/some/other/thing/is_in/here.java', 0, None, None, None),
            (4, '/some/other/thing/is_in/here.too.java', 0, None, None, None),
            (6, '/some/other/thing_another/is_folder', 1, None, None, None),
            (1, '/somebody.twat2', 1, None, None, None)],
            list(self.conn.execute("SELECT * FROM fsobject ORDER BY fullpath")))
        self.assertEqual([
            ('', None, 1, None),
            ('/some', None, 1, ''),
            ('/some/other', None, 1, '/some'),
            ('/some/other/thing', None, 1, '/some/other'),
            ('/some/other/thing/is_folder', 5, 1, '/some/other/thing'),
            ('/some/other/thing/is_in', None, 1, '/some/other/thing'),
            ('/some/other/thing/is_in/here.java', 3, 0, '/some/other/thing/is_in'),
            ('/some/other/thing/is_in/here.too.java', 4, 0, '/some/other/thing/is_in'),
            ('/some/other/thing_another', None, 1, '/some/other'),
            ('/some/other/thing_another/is_folder', 6, 1, '/some/other/thing_another'),
            ('/somebody.twat2', 1, 1, '')],
            list(self.conn.execute("SELECT * FROM folder_structure ORDER BY fullpath")))

        self.conn.execute(
            "DELETE FROM fsobject WHERE fullpath = '/some/other/thing/is_in/here.java'")
        self.conn.execute(
            "DELETE FROM fsobject WHERE fullpath = '/some/other/thing/is_in/here.too.java'")

        self.assertEqual([
            (5, '/some/other/thing/is_folder', 1, None, None, None),
            (6, '/some/other/thing_another/is_folder', 1, None, None, None),
            (1, '/somebody.twat2', 1, None, None, None)],
            list(self.conn.execute("SELECT * FROM fsobject ORDER BY fullpath")))
        self.assertEqual([
            ('', None, 1, None),
            ('/some', None, 1, ''),
            ('/some/other', None, 1, '/some'),
            ('/some/other/thing', None, 1, '/some/other'),
            ('/some/other/thing/is_folder', 5, 1, '/some/other/thing'),
            ('/some/other/thing_another', None, 1, '/some/other'),
            ('/some/other/thing_another/is_folder', 6, 1, '/some/other/thing_another'),
            ('/somebody.twat2', 1, 1, '')],
            list(self.conn.execute("SELECT * FROM folder_structure ORDER BY fullpath")))