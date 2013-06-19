from opal.core import OpalClient


class ServiceProxy:

    def __init__(self, server, user, password):
        self.client = OpalClient.buildWithAuthentication(server=server, user=user, password=password)

    def buildRequest(self):
        return self.client.new_request().fail_on_error().verbose()



