from oit.support.core import Scenarios, Scenario


class GenericClassFactory:
    @staticmethod
    def create(className, properties):
        instance = GenericClassFactory.__createInstance(className)

        # set test class attributes
        for property in properties:
            setattr(instance, property, properties[property])

        return instance()

    @staticmethod
    def __createInstance(className):
        parts = className.split('.')
        module = ".".join(parts[:-1])
        m = __import__(module)
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m


class ScenariosFactoryParser():
    requestCommandBuilder = None

    @staticmethod
    def parse(scenariosData):
        scenarios = Scenarios()
        for scenario in scenariosData:
            ScenariosFactoryParser.ScenarioParser.parse(scenarios, scenariosData[scenario])
        return scenarios

    class TestsParser:
        @staticmethod
        def parse(scenario, testsData):
            for package in testsData:
                for testClass in testsData[package]:
                    className = "oit.scenarios.%s.%s" % (package, testClass)
                    test = GenericClassFactory.create(className, testsData[package][testClass])
                    test.setRequestCommandBuilder(ScenariosFactoryParser.requestCommandBuilder)
                    scenario.addTest(test)

    class ScenarioParser():
        @staticmethod
        def parse(scenarios, scenarioData):
            scenario = Scenario(scenarioData['name'])
            ScenariosFactoryParser.TestsParser.parse(scenario, scenarioData['tests'])
            scenarios.addScenario(scenario)
            return scenario

