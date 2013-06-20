import json
from oit.support.core import AbstractTest
from oit.support.rest import JsonTableCreateCommand, HibernateDatasourceCreateCommand, DatasourcesListCommand, FileUploadCommand, ExcelTransientDatasourceCreateCommand, TablesListCommand, TableDeleteCommand, DatasourceDeleteCommand, DatasourceCompareCommand, JsonFileTableCreateCommand
from oit.support.util import JsonUtil


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
                tablesJsonResponse = JsonUtil.loads(response.content)

                for tableData in tablesJsonResponse:
                    table = tableData['name']
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
        self.requestCommandBuilder.build(JsonFileTableCreateCommand, dsName=self.dsName, file=self.file).execute()


class CreateTableFromExcel(AbstractTest):
    def run(self, data):
        file = self.file
        remote = self.remote
        dsName = self.dsName

        self.requestCommandBuilder.build(FileUploadCommand, localFile=file, opalPath=remote).execute()
        response = self.requestCommandBuilder.build(ExcelTransientDatasourceCreateCommand, file=file,
                                                    remote=remote).execute()
        jsonResponse = JsonUtil.loads(response.content)
        transientName = jsonResponse['name']
        response = self.requestCommandBuilder.build(DatasourceCompareCommand, transientName=transientName,
                                                    dsName=dsName).execute()

        jsonResponse = JsonUtil.loads(response.content)
        tableInfo = {'name': jsonResponse['withDatasource']['table'][0], 'entityType': 'Participant',
                     'variables': jsonResponse['tableComparisons'][0]['newVariables']}

        jsonData = json.dumps(tableInfo)
        self.requestCommandBuilder.build(JsonTableCreateCommand, dsName=self.dsName, jsonData=jsonData).execute()

