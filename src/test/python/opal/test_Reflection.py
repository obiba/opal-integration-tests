import unittest
from oit.scenarios.scenario import Scenarios, Scenario
from oit.support.factory import ScenariosFactoryParser
from oit.support.util import FileUtil


class TestReflection(unittest.TestCase):

    def test_createClassesFromConfigFile(self):
        config = FileUtil.loadConfig("../../resources/opal/tests.conf")
        ScenariosFactoryParser.serviceProxy = None
        scenarios = ScenariosFactoryParser.parse(config['scenarios'])
        self.assertIsInstance(scenarios, Scenarios)
        self.assertEquals(len(scenarios.list), 1)
        self.assertIsInstance(scenarios.list[0], Scenario)
        self.assertEquals(len(scenarios.list[0].tests), 3)