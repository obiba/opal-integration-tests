import time


class TestInterface:
    def run(self, data):
        pass


class AbstractTest(TestInterface):
    def setRequestCommandBuilder(self, requestCommandBuilder):
        self.requestCommandBuilder = requestCommandBuilder
        return self

    def setNext(self, test):
        self.next = test

    def run(self, data):
        pass

    def prepare(self):
        return self

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
            t = time.time()
            print "\t\tRunning test %s " % test.__class__.__name__
            test.run(data)
            print "\t\tCompeted test %.3f " % (time.time()-t)


class AbstractCommand:
    def __init__(self):
        self.sibling = None

    def execute(self):
        pass

    def executeChain(self):
        self.execute()
        if self.sibling is not None:
            self.sibling.executeChain()

    def setNext(self, sibling):
        self.sibling = sibling
        return sibling
