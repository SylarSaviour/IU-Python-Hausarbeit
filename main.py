"""
This program consists of 3 Steps:
1. Uses four "train" datasets (A) to find the four best fits from another dataset of 50 "ideal" functions (C) /
2. Allocate the identified 4 ideal function to the data point of "test" with the lowes discrepancy.
3. Visualize the Data

The following criteria are considered:
1. The criterion for selecting ideal functions for the training data set is
    to minimize the sum of all squared y-deviations.
2. The program uses the test data set (B) to validate the selection.
    Here, for each x-y pair in the test data set, it checks whether the values fit the four ideal functions.
2a. Use a criterion that ensures that the maximum deviation between the previously determined ideal function
    and the test values does not exceed the maximum deviation between the training data (A)
    and the four ideal functions from (C) by more than a factor of the square root of two (sqrt(2)).
2b. If the test data is adaptable to the four functions you found,
    save the corresponding deviations for each test data set.
3. All data are visualized logically.
4. Unit tests are written wherever possible.

Structure of the Program:
- Object Oriented (OOP)
- 1 Inheritance
- utilization of SQLite Database to store date
- Standard and user defined exceptions
- Use of libraries like pandas, matlibplot etc.
- Unite test wherever this is suitable
- Full program documentation using docstrings
- Use Guit for version control
"""

"""
START: pip install and import required libraries
"""

import pandas as pd
import sqlalchemy as db
import sqlite3
import csv
import matplotlib.pyplot as plt

"""
GET csv data into python objects. Datasetes should be stored in program folder.
"""

# Reading data from csv files using pandas
# data_train = pd.read_csv('Datensatz/train.csv')
# data_test = pd.read_csv('Datensatz/test.csv')
# data_ideal = pd.read_csv('Datensatz/ideal.csv')

"""
With the following code we create an sqlite database in project folder / 
connect th program to the database and create sqlite tables /
"""
# create inital sqlite database in project folder
sqlite3.connect("Hausarbeit.db")

class SQL_Data():
    """
    This class controlles the creation, access and update of sqlite tables
    """

    # connect database using sqlite3 to modify existing sql tables
    con = sqlite3.connect("Hausarbeit.db")
    cur = con.cursor()

    # connect database using sqlalchemy for initial table creation from csv
    con_str = "sqlite:///Hausarbeit.db"
    engine = db.create_engine(con_str)
    connection = engine.connect()
    def create_sql_table_from_csv(self, csv_path, sql_tablename):
        """
        Enter the 'csv_path'(string) object and the new 'sql_tablename'(string) to transform the one into the other
        The exception handling ensure that the program gets executed although a table with entered name already exists.
        """
        csv_data = pd.read_csv(f'{csv_path}')
        try:
            csv_data.to_sql(sql_tablename, self.engine, index=False, if_exists="fail") == ValueError
            pass
        except:
            print(f"sql table with name '{sql_tablename}' already exist and will not be replaced")
            pass
        finally:
            pass

    def add_table_column(self, table_name, new_column_name):
        """
        with this funktion new columns can be added to an existing sqlite database
        :param column_name:
        :return:
        """
        try:
            self.cur.execute(f"ALTER TABLE {table_name} ADD {new_column_name}")
            pass
        except:
            print(f"new column '{new_column_name}' in table '{table_name}' already exist and will not be replaced")
            pass
        finally:
            pass

    def update_values(self, table_name, column_name_entry, value = None,
                      column_name_x = 'x', column_value_x = None,
                      column_name_y ='y', column_value_y = None):
        """
        with this funktion data can be inserted in a column of a sqlite database table
        :param column_name:
        :return:
        """
        self.cur.execute(f"UPDATE {table_name} SET {column_name_entry}={value} WHERE {column_name_x}={column_value_x} AND {column_name_y}={column_value_y}")
        self.con.commit()

    def select_values(self, table_name, column_name, value):
        """
        with this funktion data from a databse can be selected
        :param column_name:
        :return:
        """
        self.cur.execute(f"SELECT * FROM {table_name} WHERE {column_name}={value}")
        result = self.cur.fetchall()

        for row in result:
            print(row)


# Create sqlite database by entering existing data object and to be created table name as string
data = SQL_Data()
data.create_sql_table_from_csv(csv_path="Datensatz/train.csv", sql_tablename="train")
data.create_sql_table_from_csv(csv_path="Datensatz/ideal.csv", sql_tablename="ideal")
data.create_sql_table_from_csv(csv_path="Datensatz/test.csv", sql_tablename="test")

# Add new column to test table that did not exist before, Exception handling required
data.add_table_column(table_name="test", new_column_name="Delta_Y")
data.add_table_column(table_name="test", new_column_name="Nummer_der_Idealen_Funktion")


# Test the data update function
# data.update_values(table_name="test",
#                    column_name_entry="Nummer_der_Idealen_Funktion",
#                    value=1,
#                    column_value_x=-11.1,
#                    column_value_y=-11.129228)

#Test data selection
# data.select_values(table_name="test", column_name="Nummer_der_Idealen_Funktion", value=1)





"""
COMPUTE_1: identify the four ideal function from data_ideal that fit best to data_train / 
where the deviations are less than sqrt(2)
"""

# idealfunc_1 = none
# idealfunc_2 = none
# idealfunc_3 = none
# idealfunc_4 = none

