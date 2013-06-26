import MySQLdb
from oit.support.core import AbstractCommand
from oit.support.util import MySqlUtil


class AbstractRequestCommand(AbstractCommand):
    def __init__(self, host, user, password, database=None):
        AbstractCommand.__init__(self)
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.db = MySQLdb.connect(self.host, self.user, self.password)


class DeleteDatabaseCommand(AbstractRequestCommand):
    def __init__(self, host, user, password, database):
        AbstractRequestCommand.__init__(self, host, user, password, database)

    def execute(self):
        try:
            cursor = self.db.cursor()
            cursor.execute("DROP DATABASE IF EXISTS %s" % self.database)
            self.db.commit()
        except:
            self.db.rollback()


class CreateDatabaseCommand(AbstractRequestCommand):
    def __init__(self, host, user, password, database):
        AbstractRequestCommand.__init__(self, host, user, password, database)

    def execute(self):
        try:
            cursor = self.db.cursor()
            cursor.execute("CREATE DATABASE %s" % self.database)
            self.db.commit()
        except Exception, e:
            self.db.rollback()


class ImportDatabaseCommand(AbstractRequestCommand):
    def __init__(self, host, user, password, database, sqlFile):
        AbstractRequestCommand.__init__(self, host, user, password, database)
        self.sqlFile = sqlFile

    def execute(self):
        try:
            self.db.select_db(self.database)
            # TODO for now there are a few extra steps that should be revised in the future:
            # - join all the statements on one line
            # - make sure to close the cursor after each execution to prevent MySQL error 2014
            statements = MySqlUtil.joinStatements(self.sqlFile)
            for statement in statements:
                cursor = self.db.cursor()
                cursor.execute(statement)
                cursor.close()

            self.db.commit()
        except Exception, e:
            self.db.rollback()
