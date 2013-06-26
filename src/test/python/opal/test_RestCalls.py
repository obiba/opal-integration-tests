import unittest
from oit.support.database import DeleteDatabaseCommand, CreateDatabaseCommand, ImportDatabaseCommand
from oit.support.rest import ServiceProxy, RequestCommandBuilder, FileUploadCommand, CreateOpalDatabaseCommand, DeleteOpalDatabaseCommand


class TestRestCalls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        serverProxy = ServiceProxy(server='http://localhost:8080',
                                   user='administrator',
                                   password='password',
                                   verbose=True)
        setattr(cls, 'serviceProxy', serverProxy)

    def test_requestCommandBuilderFileUpload(self):
        builder = RequestCommandBuilder(self.serviceProxy)
        builder.build(FileUploadCommand, localFile='../../resources/opal/DummyTable.xls', opalPath='/tmp').execute()

    def test_dbConfig(self):
        try:
            builder = RequestCommandBuilder(self.serviceProxy)
            sqlFile = '../../resources/opal/limesurvey.sql'
            DeleteDatabaseCommand(host='localhost', user='root', password='1234', database='Limbo').execute()
            CreateDatabaseCommand(host='localhost', user='root', password='1234', database='Limbo').execute()
            ImportDatabaseCommand(host='localhost', user='root', password='1234', database='Limbo',
                                  sqlFile=sqlFile).execute()

            builder.build(CreateOpalDatabaseCommand, dbName='Limbo', dbHost='localhost',
                          dbUrl='jdbc:mysql://localhost:3306/Limbo?characterEncoding=UTF-8', dbDriver='com.mysql.jdbc.Driver', dbUser='root',
                          dbPassword='1234').execute()

            # builder.build(DeleteOpalDatabaseCommand, dbName='Limbo').execute()

        except Exception, e:
            self.fail(e)


class ArgsTest:
    def test(self):
        self.funcA(a=12, b=34)

    def funcA(self, **kwargs):
        self.funcB(**kwargs)

    def funcB(self, a, b):
        print "A: %d, B: %d" % (a, b)