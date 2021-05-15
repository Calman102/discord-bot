import discord
from discord.ext import commands
import sqlite3
from datetime import datetime


def timestamp():
    """ Returns the current date and time as a string. """
    return datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")


class UserDatabase:
    """ A database designed to store data about users on a Discord server. """
    def __init__(self, filename):
        """
        Establishes a pre-existing database.
        :param filename: (str) the filename of an sqlite database.
        """
        self.filename = filename
        self.db = None
        self.cols = {}

    def connect(self):
        """ Connects to the database. """
        if self.db is None:
            self.db = sqlite3.connect(self.filename)

    def disconnect(self):
        """ Disconnects from the database. """
        if self.db is not None:
            self.db.close()
            self.db = None

    def backup(self):
        """ Makes a backup of the database. """
        self.connect()
        self.db.backup(f"{self.filename.split('.')[0]}_BACKUP.{self.filename.split('.')[1]}")
        print(f"[{timestamp()}] Backed up {self.filename}.")
        self.disconnect()

    def create_table(self, name, primary_key, *args):
        """ 
        Creates a table in the database if it does not exist.
        :param name: (str) name of the table.
        :param primary_key: (tuple) a tuple pair containing the name and data type
                            defining the primary key, both strings.
        :param *args: (tuple) any other name and data type pairs to add to the database.
        """
        self.cols[name] = (primary_key[0], ) + tuple(arg[0] for arg in args)
        fields = [f"{field} {field_type} NOT NULL" for field, field_type in args]
        create_table_sql = f"""CREATE TABLE IF NOT EXISTS {name} (
            {primary_key[0]} {primary_key[1]} PRIMARY KEY {",".join([""] + fields)}
        );"""
        print(create_table_sql)
        self.connect()
        self.db.execute(create_table_sql)
        self.db.commit()
        self.disconnect()

    def insert(self, table, user_id, user_data, _batch=False):
        """
        Inserts a new row into the table.
        :param table: (str) the table to insert into.
        :param user_id: (int) the ID of the user.
        :param user_data: (dict) the user data to insert.
        """
        for k, v in user_data.items():
            user_data[k] = f"'{v}'" if type(v) == str else v
        ordering = self.cols[table]
        ordered_vals = ",". join([""] + [str(user_data[k]) for k in ordering[1:]])
        insertion_sql = f"""INSERT INTO {table}
        VALUES ({user_id} {ordered_vals});"""
        print(insertion_sql)
        if not _batch:
            self.connect()
        self.db.execute(insertion_sql)
        if not _batch:
            self.db.commit()
            self.disconnect()
        
    def update(self, table, user_id, user_data, _batch=False):
        """
        Updates values in a single row of a table.
        :param table: (str) the table in the database.
        :param user_id: (int) the ID of the user.
        :param user_data: (dict) the updated user data.
        """
        for k, v in user_data.items():
            user_data[k] = f"'{v}'" if type(v) == str else v
        updates = ",".join([f"{k} = {v}" for k, v in user_data.items()])
        update_sql = f"""UPDATE {table}
        SET {updates}
        WHERE {self.cols[table][0]} = {user_id};"""
        print(update_sql)
        if not _batch:
            self.connect()
        self.db.execute(update_sql)
        if not _batch:
            self.db.commit()
            self.disconnect()
    
    def batch_insert(self, table, dump):
        """
        Inserts data into a table with the values from its dictionary representation
        in a single connection.
        :param table: (str) the table in the database.
        :param dump: (dict) the dictionary of data to insert into the table.
        """
        self.connect()
        for k, v in dump.items():
            self.insert(table, k, v, _batch=True)
        self.db.commit()
        self.disconnect()       
    
    def batch_update(self, table, dump):
        """
        Updates a table with the values in its dictionary representation in a
        single connection.
        :param table: (str) the table in the database.
        :param dump: (dict) the dictionary of data to update the table with.
        """
        self.connect()
        for k, v in dump.items():
            self.update(table, k, v, _batch=True)
        self.db.commit()
        self.disconnect()       
    
    def to_dict(self, table):
        """
        Returns a dictionary representation of a table in the database.
        :param table: (str) name of the table.
        :return: (dict) the table as a dictionary.
        """
        self.connect()
        self.db.row_factory = sqlite3.Row
        vals = self.db.execute(f"SELECT * FROM {table}").fetchall()
        d = {v[v.keys()[0]]: {k: v[k] for k in v.keys()[1:]} for v in vals}
        return d


        
            
