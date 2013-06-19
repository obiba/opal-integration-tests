class TestInterface:
    def run(self, data):
        pass


class AbstractTest(TestInterface):
    def setServiceProxy(self, proxy):
        self.serviceProxy = proxy
        return self

    def setNext(self, test):
        self.next = test

    def run(self, data):
        pass

    def appendData(self, data, key, value):
        if data is None:
            data = {}
        data[key] = value


class Scenarios(TestInterface):
    def __init__(self):
        self.list = []

    def addScenario(self, test):
        self.list.append(test)

    def run(self, data):
        print "Running sceranios..."
        for scenario in self.list:
            scenario.run(data)


class Scenario(TestInterface):
    def __init__(self, name):
        self.tests = []
        self.name = name

    def addTest(self, test):
        self.tests.append(test)

    def run(self, data):
        print "\tRunning sceranio %s" % getattr(self, 'name')

        for test in self.tests:
            print "\t\tRunning test %s " % test.__class__.__name__
            test.run(data)
