import json
from opal.file import OpalFile
from opal.protobuf import Magma_pb2
import os
from oit.support.core import AbstractTest
from oit.support.util import FileUtil


class CreateDatasource(AbstractTest):

    def run(self):
        request = self.serviceProxy.buildRequest().accept_protobuf().content_type_protobuf()

        # build transient datasource factory
        factory = Magma_pb2.DatasourceFactoryDto()
        hibernateFactory = factory.Extensions[Magma_pb2.HibernateDatasourceFactoryDto.params]
        factory.name = getattr(self, 'name')
        hibernateFactory.database = getattr(self, 'type')

        # send request and parse response as a datasource
        # request.post().resource('/datasources').content(factory.SerializeToString()).send()


class FindDatasource(AbstractTest):

    def run(self):
        request = self.serviceProxy.buildRequest().accept_json()

        # get the list of datasources
        response = request.get().resource('/datasources').send()
        jsonResponse = json.loads(response.content)
        dsName = getattr(self, 'name')

        for datasource in jsonResponse:
            if datasource['name'] == dsName:
                return
        raise Exception("Datasource %s not found" % dsName)


class CreateTable(AbstractTest):

    def run(self):
        content = FileUtil.loadJsonAsString(getattr(self, 'file'))
        request = self.serviceProxy.buildRequest().accept_json().content_type_json()
        request.post().resource('/datasource/DummyDatasource/tables').content(content).send()
