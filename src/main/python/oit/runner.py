import os
from oit.support.factory import TestClassFactory
from oit.support.rest import ServiceProxy
from oit.support.util import FileUtil


class TestRunner:
    CONFIG_FILE = os.path.join(os.path.dirname(__file__), "tests.conf")

    def run(self):
        try:
            self.__initialize()
            self.__buildScenarios()
            self.__reportSuccess()
        except Exception, e:
            self.__reportFailure(e)

    def __initialize(self):
        self.config = FileUtil.loadConfig(self.CONFIG_FILE)
        self.serviceProxy = ServiceProxy(self.config['opal']['server'], self.config['opal']['user'],
                                         self.config['opal']['password'])

    def __buildScenarios(self):
        scenarios = self.config['scenarios']

        for scenario in scenarios:
            for test in scenarios[scenario]:
                className = "oit.scenarios.%s.%s" % (scenario, test)
                test = TestClassFactory.create(className, scenarios[scenario][test])
                test.setServiceProxy(self.serviceProxy)
                test.run()

    def __reportSuccess(self):
        print '*' * 80
        print "* SUCCESS"
        print "All test passed!"
        print '*' * 80

    def __reportFailure(self, e):
        print '*' * 80
        print "* ERROR"
        print e
        print '*' * 80


if __name__ == '__main__':
    TestRunner().run()

