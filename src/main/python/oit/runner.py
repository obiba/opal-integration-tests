from datetime import datetime
import os
import traceback
from oit.support.factory import ScenariosFactoryParser
from oit.support.rest import ServiceProxy, RequestCommandBuilder
from oit.support.util import FileUtil


class TestRunner:
    CONFIG_FILE = os.path.join(os.path.dirname(__file__), "tests.conf")

    def run(self):
        try:
            self.__initialize()
            self.__reportStart()
            ScenariosFactoryParser.requestCommandBuilder = self.requestCommandBuilder
            ScenariosFactoryParser.parse(self.config['scenarios']).run({})
            self.__reportSuccess()

        except Exception, e:
            self.__reportFailure(e)

    def __initialize(self):
        self.config = FileUtil.loadConfig(self.CONFIG_FILE)
        self.serviceProxy = ServiceProxy(server=self.config['proxy']['server'],
                                         user=self.config['proxy']['user'],
                                         password=self.config['proxy']['password'],
                                         verbose=self.config['proxy']['verbose'])
        self.requestCommandBuilder = RequestCommandBuilder(self.serviceProxy)

    def __reportStart(self):
        print '*' * 80
        print "# Opal Integration Test Started @ (%s)\n" % datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def __reportSuccess(self):
        print '*' * 80
        print "*** SUCCESS ***"
        print "All test passed!"
        print '*' * 80

    def __reportFailure(self, e):
        print '*' * 80
        print "*** FAILURE ***"
        print e
        print traceback.format_exc()
        print '*' * 80


if __name__ == '__main__':
    TestRunner().run()

