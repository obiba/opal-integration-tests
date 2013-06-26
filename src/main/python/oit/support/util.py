import json
import os
import re
import unicodedata
import yaml
import oit


class FileUtil:
    @staticmethod
    def getDataFile(fileName):
        return os.path.join(os.path.dirname(oit.__file__), 'data', fileName)

    @staticmethod
    def loadJson(fileName):
        jsonContent = open(FileUtil.getDataFile(fileName)).read()
        return json.loads(jsonContent)

    @staticmethod
    def loadJsonAsString(fileName):
        return FileUtil.loadFileWithoutWhiteSpace(FileUtil.getDataFile(fileName))

    @staticmethod
    def loadConfig(fileName):
        configFile = open(fileName, 'r')
        config = yaml.load(configFile)
        configFile.close()
        return config

    @staticmethod
    def loadFileWithoutWhiteSpace(fileName):
        jsonContent = ''
        jsonFile = open(fileName, "r")
        for line in jsonFile:
            cleanedLine = line.strip()
            if cleanedLine:
                jsonContent += cleanedLine
        jsonFile.close()
        return jsonContent


class JsonUtil:
    @staticmethod
    def normalize(value):
        return unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')

    @staticmethod
    def loads(data):
        return json.loads(data, object_hook=JsonUtil._decode_dict)

    @staticmethod
    def dumps(data):
        return json.dumps(data)

    @staticmethod
    def _decode_list(data):
        rv = []
        for item in data:
            if isinstance(item, unicode):
                item = item.encode('utf-8')
            elif isinstance(item, list):
                item = JsonUtil._decode_list(item)
            elif isinstance(item, dict):
                item = JsonUtil._decode_dict(item)
            rv.append(item)
        return rv

    @staticmethod
    def _decode_dict(data):
        rv = {}
        for key, value in data.iteritems():
            if isinstance(key, unicode):
                key = key.encode('utf-8')
            if isinstance(value, unicode):
                value = value.encode('utf-8')
            elif isinstance(value, list):
                value = JsonUtil._decode_list(value)
            elif isinstance(value, dict):
                value = JsonUtil._decode_dict(value)
            rv[key] = value
        return rv


class MySqlUtil:
    @staticmethod
    def joinStatements(sqlFile):
        statements = []
        statement = ""
        with open(sqlFile, 'r') as sqlContent:
            for line in sqlContent:

                if re.search(r'^--', line, re.M) or re.search(r'^\s+$', line, re.M):
                    continue

                statement += line.strip()

                if re.search(r';', line, re.M):
                    statements.append("%s" % statement)
                    statement = ""

        sqlContent.close()

        return statements