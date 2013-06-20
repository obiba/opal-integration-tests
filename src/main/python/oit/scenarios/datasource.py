import json
from opal.protobuf import Magma_pb2
import sys
import unicodedata
from oit.support.core import AbstractTest
from oit.support.rest import JsonTableCreateCommand, HibernateDatasourceCreateCommand, DatasourcesListCommand, FileUploadCommand, ExcelTransientDatasourceCreateCommand, TablesListCommand, TableDeleteCommand, DatasourceDeleteCommand
from oit.support.util import FileUtil


class CreateDatasource(AbstractTest):

    def run(self, data):
        self.requestCommandBuilder.build(HibernateDatasourceCreateCommand, dsName=self.dsName, type=self.type).execute()


class DeleteDatasource(AbstractTest):

    def run(self, data):
        response = self.requestCommandBuilder.build(DatasourcesListCommand).execute()
        dsJsonResponse = json.loads(response.content)

        for datasource in dsJsonResponse:

            if datasource['name'] == self.dsName:
                response = self.requestCommandBuilder.build(TablesListCommand, dsName=self.dsName).execute()
                tablesJsonResponse = json.loads(response.content)

                for tableData in tablesJsonResponse:
                    table = unicodedata.normalize('NFKD', tableData['name']).encode('ascii','ignore')
                    self.requestCommandBuilder.build(TableDeleteCommand, dsName=self.dsName, table=table).execute()

                self.requestCommandBuilder.build(DatasourceDeleteCommand, dsName=self.dsName).execute()


class FindDatasource(AbstractTest):

    def run(self, data):
        response = self.requestCommandBuilder.build(DatasourcesListCommand).execute()
        jsonResponse = json.loads(response.content)

        for datasource in jsonResponse:
            if datasource['name'] == self.dsName:
                return
        raise Exception("Datasource %s not found" % self.dsName)


class CreateTableFromJson(AbstractTest):

    def run(self, data):
        self.requestCommandBuilder.build(JsonTableCreateCommand, dsName=self.dsName, file=self.file).execute()


class CreateTableFromExcel(AbstractTest):
    def run(self, data):
        file = self.file
        remote = self.remote
        dsName = self.dsName

        self.requestCommandBuilder.build(FileUploadCommand, localFile=file, opalPath=remote).execute()
        res = self.requestCommandBuilder.build(ExcelTransientDatasourceCreateCommand, file=file, remote=remote).execute()
        print res
        # response = self.requestCommandBuilder.build(DatasourceCompareCommand, transientName=jsonResponse['name'], dsName=dsName).execute()


