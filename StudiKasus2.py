import mysql.connector as mysql
from mysql.connector import Error
import sqlalchemy
from urllib.parse import quote_plus as urlquote
import matplotlib.pyplot as plt
import pandas as pd
import os

class StudiKasus2: #This class is for 
    def __init__(self, host, port, user, password): # This method is the constructor of the class. This will be called everytime the object of this class is made. The usage of this method is to connect to the database
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def create_db(self, db_name): # The usage of this method is to create a new database
        conn = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password
        )
        try:
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("CREATE DATABASE {}".format(db_name))
        except Error as e:
            print("Error while connecting to MySQL", e)
        # preparing a cursor object
        # creating database



    def create_table(self, db_name, table_name, df): # The usage of this method is to create a new table inside the specified database
        conn = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password
        )
        try:
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("USE {}".format(db_name))
                cursor.execute("CREATE TABLE {}".format(table_name))
        except Error as e:
            print("Error while connecting to MySQL", e)

        engine_stmt = 'mysql+mysqldb://%s:%s@%s:%s/%s' % (self.user, urlquote(self.password),
                                                            self.host, self.port, db_name)
        engine = sqlalchemy.create_engine(engine_stmt)

        df.to_sql(name=table_name, con=engine,
                  if_exists='append', index=False, chunksize=1000)

    def load_data(self, db_name, table_name): # The usage of this method is to load all the data from the specified table inside the database. This method will return all the datas in form of a list
        conn = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password
        )
        try:
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM {}.{}".format(db_name, table_name))
                result = cursor.fetchall()
                return result
        except Error as e:
            print("Error while connecting to MySQL", e)



    def import_csv(self, path): # The usage of this method is to import all the data from a csv file. This method will return all the data in form of dataframe

        return pd.read_csv(path, index_col=False, delimiter=',')