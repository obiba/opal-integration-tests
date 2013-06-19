class TestClassFactory:

    @classmethod
    def create(cls, className, properties):
        instance = TestClassFactory.__createInstance(className)

        # set test class attributes
        for property in properties:
            setattr(instance, property, properties[property])

        return instance()

    @classmethod
    def __createInstance(cls, className):
        parts = className.split('.')
        module = ".".join(parts[:-1])
        m = __import__(module)
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m