import json
import os
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