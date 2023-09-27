import mysql.connector
import time
from db_config.db_environ import Config


class dbConnect():
    def __init__(self, limit_retries, reconnect):
        self.param_dict = {
            "host": Config.HOST,
            "port": Config.PORT,
            "database": Config.DB,
            "user": Config.USER,
            "password": Config.PASSWORD
        }
        self.database_url = Config.DATABASE_URL
        self._connection = None
        self._cursor = None
        self.reconnect = reconnect
        self.limit_retries = limit_retries
        self.ignite()

    def ignite(self):
        self.connect()
        self.myCursor()

    def reset(self):
        self.close()
        self.ignite()

    def connect(self, retry_counter=0):
        try:
            if self.database_url != '':
                self._connection = mysql.connector.connect(self.database_url)
            else:
                self._connection = mysql.connector.connect(**self.param_dict)
                    
            retry_counter = 0
            self._connection.autocommit = False
                
        except mysql.connector.Error as error:
            if not self.reconnect or retry_counter >= self.limit_retries:
                raise error
            else:
                retry_counter += 1
                time.sleep(5)
                    
                self.connect(retry_counter)
        except (Exception, mysql.connector.Error) as error:
            raise error

        return self._connection
    
    def myCursor(self):
        if self._cursor == None or self._cursor.closed:
            self._cursor = self._connection.cursor()

    def execute(self, str_query, retry_counter=0):
        try:
            self._cursor.execute(str_query)
            self._connection.commit()
            retry_counter = 0

        except:
            retry_counter += 1
            time.sleep(1)
            
            self.reset()

            # Try execute query after reseting connection
            self.execute(str_query, retry_counter)
            self._connection.commit()
    
    def close(self):
        if self._connection:
            if self._cursor:
                self._cursor.close()
            self._connection.close()
            
        self._connection = None
        self._cursor = None