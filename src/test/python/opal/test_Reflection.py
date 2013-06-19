import unittest
import yaml
from oit.runner import TestRunner
from oit.scenarios.datasource import CreateDatasource
from oit.support.factory import TestClassFactory


class TestReflection(unittest.TestCase):

    def test_createClassesFromConfigFile(self):
        config = self.__loadConfig(TestRunner().CONFIG_FILE)
        scenarios = config['scenarios']

        for scenario in scenarios:
            for test in scenarios[scenario]:
                className = "oit.scenarios.%s.%s" % (scenario, test)
                test = TestClassFactory.create(className, scenarios[scenario][test])
                self.assertIs(test, CreateDatasource)

    def __loadConfig(self, configFile):
        configFile = open(configFile, 'r')
        config = yaml.load(configFile)
        configFile.close()
        return config
