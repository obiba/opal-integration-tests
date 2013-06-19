from opal.core import OpalClient


class ServiceProxy:

    def __init__(self, server, user, password, verbose):
        self.client = OpalClient.buildWithAuthentication(server=server, user=user, password=password)
        self.verbose = verbose

    def buildRequest(self):
        request = self.client.new_request().fail_on_error()
        if self.verbose:
            request.verbose()

        return request



