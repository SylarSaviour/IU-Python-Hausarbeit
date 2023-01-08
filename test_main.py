import unittest
import sqlite3


class TestSQLiteConnection(unittest.TestCase):

    # The following test checks if a sqlite database connection is established.
    # it passes if the cursor is not None, indicating that the connection was successful.
    def test_connection(self):
        conn = sqlite3.connect('Hausarbeit.db')
        cursor = conn.cursor()
        if cursor is None:
            raise ValueError("Error: Could not establish a connection to the database.")
        else:
            self.assertIsNotNone(cursor)


class TestSQLiteTables(unittest.TestCase):
    # The following test checks if the required sqlite tables has been created.
    def test_tables(self):
        conn = sqlite3.connect('Hausarbeit.db')
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        try:
            self.assertIn(('train',), tables)
            self.assertIn(('test',), tables)
            self.assertIn(('ideal',), tables)
        except AssertionError as e:
            raise ValueError("Error: One or more tables were not found in the database.") from e


class TestSQLiteColumns(unittest.TestCase):
    # The following test checks if the required columns are created in the test sqlite talbe.
    def test_columns(self):
        conn = sqlite3.connect('Hausarbeit.db')
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(test);")
        columns = [t[1] for t in cursor.fetchall()]
        try:
            self.assertIn('Delta_Y', columns)
            self.assertIn('Nummer_der_Idealen_Funktion', columns)
        except AssertionError as e:
            raise ValueError("Error: One or more columns were not found in the table.") from e


class TestSQLiteColumnCount(unittest.TestCase):
    # Test if the SQLite table of the 4 Ideal Functions has actually 4 Functions in it + the x value column
    def test_column_count(self):
        conn = sqlite3.connect('Hausarbeit.db')
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(ideal_4);")
        column_count = len([t[1] for t in cursor.fetchall()])
        if column_count != 5:
            raise ValueError("Error: ideal_4 has {} columns, expected 5.".format(column_count))


if __name__ == '__main__':
    unittest.main()
