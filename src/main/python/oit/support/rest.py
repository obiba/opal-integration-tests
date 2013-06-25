import os
from os.path import basename

from opal.core import OpalClient
from opal.file import OpalFile
from opal.protobuf import Magma_pb2

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


class TransientDatasourceCreateCommand(AbstractRequestCommand):
    def __init__(self, opalRequest, file, remote):
        AbstractRequestCommand.__init__(self, opalRequest)
        self.file = file
        self.remote = remote

    def addExtension(self, factory):
        pass

    def execute(self):
        # build transient datasource factory
        factory = Magma_pb2.DatasourceFactoryDto()
        self.addExtension(factory)

        # send request and parse response as a datasource
        self.opalRequest.accept_json().content_type_protobuf().post().resource('/transient-datasources').content(
            factory.SerializeToString())

        return self.opalRequest.send()


class ExcelTransientDatasourceCreateCommand(TransientDatasourceCreateCommand):
    def __init__(self, opalRequest, file, remote):
        TransientDatasourceCreateCommand.__init__(self, opalRequest, file, remote)

    def addExtension(self, factory):
        # build transient datasource factory
        excelFactory = factory.Extensions[Magma_pb2.ExcelDatasourceFactoryDto.params]
        excelFactory.file = os.path.join(self.remote, self.file)
        excelFactory.readOnly = True


class SpssTransientDatasourceCreateCommand(TransientDatasourceCreateCommand):
    def __init__(self, opalRequest, file, remote, characterSet, locale, entityType):
        TransientDatasourceCreateCommand.__init__(self, opalRequest, file, remote)
        self.characterSet = characterSet
        self.locale = locale
        self.entityType = entityType

    def addExtension(self, factory):
        # build transient datasource factory
        spssFactory = factory.Extensions[Magma_pb2.SpssDatasourceFactoryDto.params]
        spssFactory.file = os.path.join(self.remote, self.file)

        if self.characterSet:
            spssFactory.characterSet = self.characterSet

        if self.locale:
            spssFactory.locale = self.locale

        if self.entityType:
            spssFactory.entityType = self.entityType


class CsvTransientDatasourceCreateCommand(TransientDatasourceCreateCommand):
    def __init__(self, opalRequest, file, remote, characterSet, entityType, separator, quote, firstRow):
        TransientDatasourceCreateCommand.__init__(self, opalRequest, file, remote)
        self.characterSet = characterSet
        self.entityType = entityType
        self.separator = separator
        self.quote = quote
        self.firstRow = firstRow

    def addExtension(self, factory):
        # build transient datasource factory
        csvFactory = factory.Extensions[Magma_pb2.CsvDatasourceFactoryDto.params]

        if self.characterSet:
            csvFactory.characterSet = self.characterSet

        if self.separator:
            csvFactory.separator = self.separator

        if self.quote:
            csvFactory.quote = self.quote

        if self.firstRow:
            csvFactory.firstRow = self.firstRow

        table = csvFactory.tables.add()
        table.data = os.path.join(self.remote, self.file)
        table.entityType = self.entityType

        # if self.tables:
        #     table.name = self.tables[0]
        # else:
        # Take filename as the table name
        name = basename(self.file)

        index = name.split('.')
        if index > 0:
            table.name = name[-1][:-index]
        else:
            table.name = name[-1]

        print index, "\t", table.name


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
