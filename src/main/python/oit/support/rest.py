from opal.core import OpalClient
from opal.file import OpalFile
from opal.protobuf import Magma_pb2
import os

from oit.support.util import FileUtil


class ServiceProxy:
    def __init__(self, server, user, password, verbose):
        self.client = OpalClient.buildWithAuthentication(server=server, user=user, password=password)
        self.verbose = verbose

    def buildRequest(self):
        request = self.client.new_request().fail_on_error()
        if self.verbose:
            request.verbose()

        return request


class RequestCommandBuilder:
    def __init__(self, serviceProxy):
        self.serviceProxy = serviceProxy

    def build(self, clazz, **kwargs):
        return clazz(self.serviceProxy.buildRequest(), **kwargs)


class AbstractRequestCommand:
    def __init__(self, opalRequest):
        self.sibling = None
        self.opalRequest = opalRequest

    def execute(self):
        pass

    def next(self, sibling):
        self.sibling = sibling


class HibernateDatasourceCreateCommand(AbstractRequestCommand):
    def __init__(self, opalRequest, dsName, type):
        AbstractRequestCommand.__init__(self, opalRequest)
        self.dsName = dsName
        self.type = type

    def execute(self):
        self.opalRequest.accept_json().content_type_protobuf()

        # build transient datasource factory
        factory = Magma_pb2.DatasourceFactoryDto()
        hibernateFactory = factory.Extensions[Magma_pb2.HibernateDatasourceFactoryDto.params]
        factory.name = self.dsName
        hibernateFactory.database = self.type

        # send request and parse response as a datasource
        self.opalRequest.post().resource('/datasources').content(factory.SerializeToString())
        return self.opalRequest.send()


class ExcelTransientDatasourceCreateCommand(AbstractRequestCommand):
    def __init__(self, opalRequest, file, remote):
        AbstractRequestCommand.__init__(self, opalRequest)
        self.file = file
        self.remote = remote

    def execute(self):
        # build transient datasource factory
        factory = Magma_pb2.DatasourceFactoryDto()
        hibernateFactory = factory.Extensions[Magma_pb2.ExcelDatasourceFactoryDto.params]
        hibernateFactory.file = os.path.join(self.remote, self.file)
        hibernateFactory.readOnly = True

        # send request and parse response as a datasource
        self.opalRequest.accept_json().content_type_protobuf().post().resource('/transient-datasources').content(
            factory.SerializeToString())

        return self.opalRequest.send()


class DatasourcesListCommand(AbstractRequestCommand):
    def __init__(self, opalRequest):
        AbstractRequestCommand.__init__(self, opalRequest)

    def execute(self):
        self.opalRequest.get().accept_json().resource('/datasources')
        return self.opalRequest.send()


class DatasourceDeleteCommand(AbstractRequestCommand):
    def __init__(self, opalRequest, dsName):
        AbstractRequestCommand.__init__(self, opalRequest)
        self.dsName = dsName

    def execute(self):
        resourcePath = "/datasource/%s" % self.dsName
        self.opalRequest.delete().accept_json().resource(resourcePath)
        return self.opalRequest.send()


class DatasourceCompareCommand(AbstractRequestCommand):
    def __init__(self, opalRequest, transientName, dsName):
        AbstractRequestCommand.__init__(self, opalRequest)
        self.dsName = dsName
        self.transientName = transientName

    def execute(self):
        resourcePath = "/datasource/%s/compare/%s" % (self.transientName, self.dsName)
        self.opalRequest.accept_json().get().resource(resourcePath)

        return self.opalRequest.send()


class JsonTableCreateCommand(AbstractRequestCommand):
    def __init__(self, opalRequest, dsName, jsonData):
        AbstractRequestCommand.__init__(self, opalRequest)
        self.dsName = dsName
        self.jsonData = jsonData

    def execute(self):
        resourcePath = "/datasource/%s/tables" % self.dsName
        self.opalRequest.accept_json().content_type_json().post().resource(resourcePath).content(self.jsonData)

        return self.opalRequest.send()


class JsonFileTableCreateCommand(JsonTableCreateCommand):

    def __init__(self, opalRequest, dsName, file):
        JsonTableCreateCommand.__init__(self, opalRequest, dsName, FileUtil.loadJsonAsString(file))


class TablesListCommand(AbstractRequestCommand):

    def __init__(self, opalRequest, dsName):
        AbstractRequestCommand.__init__(self, opalRequest)
        self.dsName = dsName

    def execute(self):
        resourcePath = "/datasource/%s/tables" % self.dsName
        self.opalRequest.accept_json().get().resource(resourcePath)

        return self.opalRequest.send()


class TableDeleteCommand(AbstractRequestCommand):
    def __init__(self, opalRequest, dsName, table):
        AbstractRequestCommand.__init__(self, opalRequest)
        self.dsName = dsName
        self.table = table

    def execute(self):
        resourcePath = "/datasource/%s/table/%s" % (self.dsName, self.table)
        self.opalRequest.delete().accept_json().resource(resourcePath)

        return self.opalRequest.send()


class FileUploadCommand(AbstractRequestCommand):
    def __init__(self, opalRequest, localFile, opalPath):
        AbstractRequestCommand.__init__(self, opalRequest)
        self.localFile = localFile
        self.opalPath = opalPath

    def execute(self):
        file = OpalFile(self.opalPath)
        self.opalRequest.content_upload(FileUtil.getDataFile(self.localFile)).accept('text/html').content_type(
            'multipart/form-data').post().resource(file.get_ws())

        return self.opalRequest.send()
