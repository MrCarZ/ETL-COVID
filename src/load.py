from utils import sql_statements
import mysql.connector
from mysql.connector import errorcode
class MySQL:
    def __init__(self, host, user, password, database):
        self.host=host
        self.user=user
        self.password=password
        self.database=database
    
    def connect_to_db(self):
        try:
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            print("Connection Sucessful!")
            return connection
        except mysql.connector.Error as error:
            print('Connection to Database "%s" Failed!', (self.database, ))
            print(error)

    def disconnect_from_db(self, connection):
        if(connection.is_connected() == True):
            try:
                connection.close()
                print("Disconnected from Database!")
            except mysql.connector.Error as error:
                print("An error occurred while disconnecting from Database: %s", (error,)) 

    def create_table(self, table):
        try:
            connection = self.connect_to_db()
            cursor = connection.cursor()
            statement = sql_statements.create_table.format(table_name = table)
            cursor.execute(statement)
            
            print("Creating Table Named {table_name} ...".format(table_name = table))
            
            connection.commit()
            print("Table Created Successfully!")
        except mysql.connector.Error as error:
            print("Error in Creating Table:", error)
        finally:
            cursor.close()
            self.disconnect_from_db(connection)

    def insert_to_db(self, dataframe, table):
            try:
                connection = self.connect_to_db()
                cursor = connection.cursor()
                formatted_data = dataframe.to_numpy().tolist()
                statement = sql_statements.insert_to_table.format(table_name = table)
                cursor.executemany(statement, formatted_data)
                for data in cursor:
                    print(data)
                connection.commit()
                print("Data insertion was sucessfull!")
            except mysql.connector.Error as error:
                print("Data insertion failed: ", error)
            finally:
                cursor.close()
                self.disconnect_from_db(connection)

    def select_from_db(self, table, data):
        try:
            connection = self.connect_to_db()
            cursor = connection.cursor()
            statement = sql_statements.select_from_table.format(data=data, table_name=table)
            cursor.execute(statement)
            print("Data Selection was sucessfull!")
        except mysql.connector.Error as error:
            print("Data insertion failed: ", error)
        finally:
            cursor.close()
            self.disconnect_from_db(connection)
