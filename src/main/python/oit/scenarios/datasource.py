import os
from oit.support.core import AbstractTest
from oit.support.database import DeleteDatabaseCommand, CreateDatabaseCommand, ImportDatabaseCommand
from oit.support.rest import (HibernateDatasourceCreateCommand,
                              DatasourcesListCommand,
                              FileUploadCommand,
                              ExcelTransientDatasourceCreateCommand,
                              TablesListCommand,
                              TableDeleteCommand,
                              DatasourceDeleteCommand,
                              JsonFileTableCreateCommand,
                              ImportDataCommand,
                              DatasourceCompareCommand,
                              JsonTableCreateCommand, CreateOpalDatabaseCommand, DeleteOpalDatabaseCommand, ListOpalDatabaseCommand)

from oit.support.util import JsonUtil, FileUtil


class DeleteDatasourceTest(AbstractTest):
    def run(self, data):
        response = self.requestCommandBuilder.build(DatasourcesListCommand).execute()
        dsJsonResponse = JsonUtil.loads(response.content)

        for datasource in dsJsonResponse:

            if datasource['name'] == self.dsName:
                response = self.requestCommandBuilder.build(TablesListCommand, dsName=self.dsName).execute()
                tablesJsonResponse = JsonUtil.loads(response.content)

                for tableData in tablesJsonResponse:
                    table = tableData['name']
                    self.requestCommandBuilder.build(TableDeleteCommand, dsName=self.dsName, table=table).execute()

                self.requestCommandBuilder.build(DatasourceDeleteCommand, dsName=self.dsName).execute()


class CreateDatasourceTest(AbstractTest):
    def run(self, data):
        self.requestCommandBuilder.build(HibernateDatasourceCreateCommand, dsName=self.dsName, type=self.type).execute()


class FindDatasourceTest(AbstractTest):
    def run(self, data):
        response = self.requestCommandBuilder.build(DatasourcesListCommand).execute()
        jsonResponse = JsonUtil.loads(response.content)

        for datasource in jsonResponse:
            if datasource['name'] == self.dsName:
                return
        raise Exception("Datasource %s not found" % self.dsName)


class CreateTableFromJsonTest(AbstractTest):
    def run(self, data):
        self.requestCommandBuilder.build(JsonFileTableCreateCommand, dsName=self.dsName, file=self.file).execute()


class CreateTableFromFileTest(AbstractTest):
    def __init__(self):
        self.unit = None
        self.tables = None
        self.incremental = False

    def createExtensionFactory(self):
        pass

    def prepare(self):
        self.path = os.path.join(self.remote, self.file)

    def run(self, data):
        self.requestCommandBuilder.build(FileUploadCommand, localFile=self.file, opalPath=self.remote).execute()
        self.requestCommandBuilder.build(ImportDataCommand, dsName=self.dsName, tables=self.tables,
                                         incremental=self.incremental, unit=self.unit,
                                         extensionFactory=self.createExtensionFactory()).execute()


class CreateTableFromExcelTest(CreateTableFromFileTest):
    def run(self, data):
        self.requestCommandBuilder.build(FileUploadCommand, localFile=self.file, opalPath=self.remote).execute()
        response = self.requestCommandBuilder.build(ExcelTransientDatasourceCreateCommand, file=self.file,
                                                    remote=self.remote).execute()

        jsonResponse = JsonUtil.loads(response.content)
        transientName = jsonResponse['name']
        response = self.requestCommandBuilder.build(DatasourceCompareCommand, transientName=transientName,
                                                    dsName=self.dsName).execute()

        jsonResponse = JsonUtil.loads(response.content)
        tableInfo = {'name': jsonResponse['compared']['table'][0], 'entityType': 'Participant'}

        if 'newVariables' in jsonResponse['tableComparisons'][0]:
            tableInfo['variables'] = jsonResponse['tableComparisons'][0]['newVariables']

        jsonData = JsonUtil.dumps(tableInfo)
        self.requestCommandBuilder.build(JsonTableCreateCommand, dsName=self.dsName, jsonData=jsonData).execute()


class CreateTableFromSpssTest(CreateTableFromFileTest):
    def __init__(self):
        CreateTableFromFileTest.__init__(self)
        # optional values
        self.characterSet = None
        self.locale = None
        self.entityType = None

    def createExtensionFactory(self):
        return ImportDataCommand.createSpssExtensionFactory(characterSet=self.characterSet, path=self.path,
                                                            locale=self.locale, entityType=self.entityType)


class CreateTableFromCsvTest(CreateTableFromFileTest):
    def __init__(self):
        CreateTableFromFileTest.__init__(self)
        # optional values
        self.characterSet = None
        self.separator = ','
        self.quote = '"'
        self.firstRow = 1
        self.entityType = 'Participant'

    def createExtensionFactory(self):
        return ImportDataCommand.createCsvExtensionFactory(characterSet=self.characterSet, separator=self.separator,
                                                           quote=self.quote, firstRow=self.firstRow, path=self.path,
                                                           type=self.entityType, tables=self.tables)


class CreateTableFromOpalTest(AbstractTest):
    def run(self, data):
        extensionFactory = ImportDataCommand.createOpalExtensionFactory(self.ropal, self.rdatasource, self.ruser,
                                                                        self.rpassword)
        self.requestCommandBuilder.build(ImportDataCommand, dsName=self.dsName, tables=self.tables, incremental=None,
                                         unit=None, extensionFactory=extensionFactory).execute()


class CreateTableFromLimeSurveyTest(AbstractTest):
    def __init__(self):
        self.tables = None
        self.prefix = None

    def run(self, data):
        sqlFile = FileUtil.getDataFile(self.sqlFile)
        DeleteDatabaseCommand(host=self.dbHost, user=self.dbUser, password=self.dbPassword,
                              database=self.dbName).execute()
        CreateDatabaseCommand(host=self.dbHost, user=self.dbUser, password=self.dbPassword,
                              database=self.dbName).execute()
        ImportDatabaseCommand(host=self.dbHost, user=self.dbUser, password=self.dbPassword, database=self.dbName,
                              sqlFile=sqlFile).execute()

        self.requestCommandBuilder.build(CreateOpalDatabaseCommand, dbName=self.dbName, dbHost=self.dbHost,
                                         dbUrl=self.dbUrl, dbDriver=self.dbDriver,
                                         dbUser=self.dbUser, dbPassword=self.dbPassword).execute()

        extensionFactory = ImportDataCommand.createLimeSurveyExtensionFactory(database=self.dbName, prefix=self.prefix)
        self.requestCommandBuilder.build(ImportDataCommand, dsName=self.dsName, tables=self.tables, incremental=None,
                                         unit=None, extensionFactory=extensionFactory).execute()


class DeleteOpalDatabaseTest(AbstractTest):
    def run(self, data):
        response = self.requestCommandBuilder.build(ListOpalDatabaseCommand).execute()
        jsonResponse = JsonUtil.loads(response.content)

        for db in filter(lambda s: s['name'] == self.dbName, jsonResponse):
            self.requestCommandBuilder.build(DeleteOpalDatabaseCommand, dbName=db['name']).execute()
