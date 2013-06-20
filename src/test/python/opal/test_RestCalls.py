import unittest
from oit.support.rest import ServiceProxy, RequestCommandBuilder, FileUploadCommand


class TestRestCalls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        serverProxy = ServiceProxy(server='http://localhost:8080',
                                   user='administrator',
                                   password='password',
                                   verbose=True)
        setattr(cls, 'serviceProxy', serverProxy)

    def test_requestCommandBuilderFileUpload(self):
        builder = RequestCommandBuilder(self.serviceProxy.buildRequest())
        builder.build(FileUploadCommand, localFile='../../resources/opal/DummyTable.xls', opalPath='/tmp').execute()


class ArgsTest:
    def test(self):
        self.funcA(a=12, b=34)

    def funcA(self, **kwargs):
        self.funcB(**kwargs)

    def funcB(self, a, b):
        print "A: %d, B: %d" % (a, b)