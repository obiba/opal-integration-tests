import json
from oit.support.core import AbstractTest
from oit.support.rest import JsonTableCreateCommand, HibernateDatasourceCreateCommand, DatasourcesListCommand, FileUploadCommand, ExcelTransientDatasourceCreateCommand, TablesListCommand, TableDeleteCommand, DatasourceDeleteCommand, DatasourceCompareCommand, JsonFileTableCreateCommand, SpssTransientDatasourceCreateCommand, CsvTransientDatasourceCreateCommand
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


class CreateTableFromFile(AbstractTest):
    def buildTransientDatasource(self):
        pass

    def run(self, data):
        self.requestCommandBuilder.build(FileUploadCommand, localFile=self.file, opalPath=self.remote).execute()
        response = self.buildTransientDatasource().execute()

        jsonResponse = JsonUtil.loads(response.content)
        transientName = jsonResponse['name']
        response = self.requestCommandBuilder.build(DatasourceCompareCommand, transientName=transientName,
                                                    dsName=self.dsName).execute()

        jsonResponse = JsonUtil.loads(response.content)
        tableInfo = {'name': jsonResponse['compared']['table'][0], 'entityType': 'Participant'}

        if 'newVariables' in jsonResponse['tableComparisons'][0]:
            tableInfo['variables'] = jsonResponse['tableComparisons'][0]['newVariables']

        jsonData = json.dumps(tableInfo)
        self.requestCommandBuilder.build(JsonTableCreateCommand, dsName=self.dsName, jsonData=jsonData).execute()


class CreateTableFromExcel(CreateTableFromFile):
    def buildTransientDatasource(self):
        return self.requestCommandBuilder.build(ExcelTransientDatasourceCreateCommand, file=self.file,
                                                remote=self.remote)


class CreateTableFromSpss(CreateTableFromFile):
    def __init__(self):
        # optional values
        self.characterSet = None
        self.locale = None
        self.entityType = None

    def buildTransientDatasource(self):
        return self.requestCommandBuilder.build(SpssTransientDatasourceCreateCommand, file=self.file,
                                                remote=self.remote, characterSet=self.characterSet, locale=self.locale,
                                                entityType=self.entityType)


class CreateTableFromCsv(CreateTableFromFile):
    def __init__(self):
        # optional values
        self.characterSet = None
        self.separator = None
        self.quote = None
        self.firstRow = None
        self.entityType = 'Participant'

    def buildTransientDatasource(self):
        return self.requestCommandBuilder.build(CsvTransientDatasourceCreateCommand, file=self.file, remote=self.remote,
                                                characterSet=self.characterSet, entityType=self.entityType,
                                                separator=self.separator, quote=self.quote, firstRow=self.firstRow)
