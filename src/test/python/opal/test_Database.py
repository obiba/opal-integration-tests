import unittest
from oit.support.database import DeleteDatabaseCommand, CreateDatabaseCommand, ImportDatabaseCommand
from oit.support.util import MySqlUtil


class TestDatabase(unittest.TestCase):
    def test_deleteDatabase(self):
        dbCommand = DeleteDatabaseCommand(host='localhost', user='root', password='1234', database='Limbo')
        dbCommand.execute()

    def test_createDatabase(self):
        dbCommand = CreateDatabaseCommand(host='localhost', user='root', password='1234', database='Limbo')
        dbCommand.execute()

    def test_importDatabase(self):
        sqlFile = '../../resources/opal/limesurvey.sql'
        DeleteDatabaseCommand(host='localhost', user='root', password='1234', database='Limbo').execute()
        CreateDatabaseCommand(host='localhost', user='root', password='1234', database='Limbo').execute()
        ImportDatabaseCommand(host='localhost', user='root', password='1234', database='Limbo',
                              sqlFile=sqlFile).execute()
