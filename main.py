"""
START: pip install and import required libraries
"""

import pandas as pd
import numpy as np
import math
import sqlalchemy as db
import sqlite3
import matplotlib.pyplot as plt

"""
This program consists of 3 Steps:
1. Uses four "train" datasets (A) to find the four best fits from another dataset of 50 "ideal" functions (C) /
2. Allocate the identified 4 ideal function to the data point of "test" with the lowes discrepancy.
3. Visualize the Data

Structure of the Program:
- Standard and user defined exceptions
- Unite test wherever this is suitable
"""

"""
With the following code we create an sqlite database in project folder /
connect the program to the database and create sqlite tables /

! ! ! ATTENTION ! ! !
Please store the "Datensatz" folder with the 3 cvs files "train.csv", "ideal.csv" and "test.csv" in the project folder.
"""


class SqlData:
    """
    This class controls the creation, access and update of sqlite database and tables
    """

    db_name = "Hausarbeit.db"

    sqlite3.connect(db_name)
    print(f"Database '{db_name}' created/connected")

    # connect database using sqlite3 to modify existing sql tables
    con = sqlite3.connect(db_name)
    cur = con.cursor()

    # connect database using sqlalchemy for initial table creation from csv
    con_str = f"sqlite:///{db_name}"
    engine = db.create_engine(con_str)
    connection = engine.connect()

    def create_sql_table_from_csv(self, csv_path, sql_table_name):

        """
        with this function a sql table can be created from a csv file
         :param csv_path: path of the csv file as string
         :param sql_table_name: name of the table that will be created as string
         :return: csv file will be transformed into a sql_table
        """

        csv_data = pd.read_csv(f'{csv_path}')
        try:
            csv_data.to_sql(sql_table_name, self.engine, index=False, if_exists="fail")
            print(f"tabel '{sql_table_name}' has been created")
            pass
        except:
            print(f"SQL table '{sql_table_name}' already exist")
            pass
        finally:
            pass

    def create_sql_table_from_dataframe(self, dataframe, sql_table_name):

        """
        with this function a sql table can be created from a csv file
         :param dataframe: dataframe object to be transformed
         :param sql_table_name: name of the table that will be created as string
         :return: dataframe file will be transformed into a sql_table
        """

        try:
            dataframe.to_sql(sql_table_name, self.engine, index=False, if_exists="fail")
            print(f"tabel '{sql_table_name}' has been created")
            pass
        except:
            print(f"SQL table '{sql_table_name}' already exist")
            pass
        finally:
            pass

    def add_table_column(self, table_name, new_column_name, column_type):

        """
        with this function new columns can be added to an existing sqlite database
        :param table_name: sql table name as string
        :param new_column_name: name of the new column as string
        :param column_type: datatype of the new column as string (NULL, INTEGER, REAL, TEXT BLOB)
        :return: new column in table if not already exist
        """

        try:
            self.cur.execute(f"ALTER TABLE {table_name} ADD {new_column_name} {column_type}")
            print(f"new column '{new_column_name}' in table '{table_name}' has been created")
            pass
        except:
            print(f"new column '{new_column_name}' in table '{table_name}' already exist")
            pass
        finally:
            pass

    def update_values(self, table_name,
                      column_name_entry, value=None,
                      column_name_x='x', column_value_x=None,
                      column_name_y='y', column_value_y=None):

        """
        with this function data can be inserted in a sqlite database table
        :param table_name: sql table name to be updated as string
        :param column_name_entry: sql table column to be updated as string
        :param value: value to be updated
        :param column_name_x: default value = 'x' conditions of row to be updated column name
        :param column_value_x: default value = 'x' conditions of row to be updated column value
        :param column_name_y: conditions of row to be updated column name
        :param column_value_y: conditions of row to be updated column value
        :return: sql table is updated
        """

        self.cur.execute(f"SELECT * FROM {table_name} WHERE {column_name_x}={column_value_x}")
        result = self.cur.fetchall()

        if not result:
            raise ValueError("no matching criteria to update values in db")
        else:
            self.cur.execute(
                f"UPDATE {table_name} "
                f"SET {column_name_entry}={value} "
                f"WHERE {column_name_x}={column_value_x} "
                f"AND {column_name_y}={column_value_y}")
            self.con.commit()
            # print(f"Data Update in table: '{table_name}' column: '{column_name_entry}' value: {value}")

    def pd_from_sql(self, table_name):

        """
        with this function data from a sql table can be stored in a pandas dataframe
        :param table_name: name of the table to be transformed in a dataframe
        :return: pandas dataframe "df" from sql table
        """

        df = pd.read_sql(sql=table_name, con=self.con_str)

        return df


# Create sqlite database by entering existing data object and to be created table name as string
data = SqlData()
data.create_sql_table_from_csv(csv_path="Datensatz/train.csv", sql_table_name="train")
data.create_sql_table_from_csv(csv_path="Datensatz/ideal.csv", sql_table_name="ideal")
data.create_sql_table_from_csv(csv_path="Datensatz/test.csv", sql_table_name="test")

# Add new column to 'test' sqlite table that did not exist before
data.add_table_column(table_name="test", new_column_name="Delta_Y", column_type="REAL")
data.add_table_column(table_name="test", new_column_name="Nummer_der_Idealen_Funktion", column_type="TEXT")

# Create pandas dataframes for calculations
df_ideal = SqlData().pd_from_sql(table_name="ideal")
df_train = SqlData().pd_from_sql(table_name="train")
df_test = SqlData().pd_from_sql(table_name="test")
df_4I = pd.DataFrame()
df_4I["x"] = df_train["x"]

"""
With the following code we will do the calculations to identify the 4 ideal functions 
and allocated them to the test data if this is applicable
"""


class Compute:

    """
    With this class we identify the 4 ideal functions and allocate the best ideal function to the test dataset
    """

    def get_4_ideal_functions(self, df_1, df_2):

        """
        with this function the 4 ideal functions from ideal.csv are identified
        :param df_1: First dataframe to compare as string. E.g. 'df_train'
        :param df_2: First dataframe to compare as string. E.g. 'df_ideal'
        :return: pandas dataframe 'df_4I' with 4 ideal functions
        """

        # Get functions names (column names) from dataframes as well as amount of function entries.
        functions_train = list(df_1.columns)[1:]
        functions_ideal = list(df_2.columns)[1:]
        row_count = len(df_train.index)

        # Iterate through all 'train' functions to identify the "ideal" function
        for t in functions_train:

            # As multiple functions can fit in the range of sqrt2 difference, the 'threshold' will store the lowes
            # total difference to determine the best ideal function for a train function.
            # 100 is just the start value
            threshold = 100

            # Iterate through all 'ideal' functions for on 'train' function
            for i in functions_ideal:
                function_train = t
                function_check = i

                # Check if a datapoint of an 'ideal' function is within a difference of sqrt(2) and write 'True' in and
                # new created column called 'check'.
                df_train['check'] = np.where((df_train[function_train] - df_ideal[function_check]
                                              <= math.sqrt(2)) &
                                             (df_train[function_train] - df_ideal[function_check]
                                              >= (math.sqrt(2)) * -1), 'True', 'False')

                # Store the difference of every datapoint in a new column called 'dif'
                df_train['dif'] = df_train[function_train] - df_ideal[function_check]

                # Count how many times an ideal function was within the range of sqrt(2)
                # with they keyword 'True' in column 'check
                result = df_train[df_train['check'] == 'True'].count()

                # sum-up all differences of the 'ideal' function compared to the 'train' function
                sum_dif = abs(df_train['dif'].sum())

                # Check if the 'ideal' functions has all datapoints with 'check' to be considered as ideal function
                if result['check'] == row_count:

                    # check, if the total difference is smaller than the threshold
                    # if this is the case, overwrite the threshold with the own value to be considered as currently the
                    # best ideal function for the training functions
                    # if ideal function is not smaller than the threshold, it is not considered anymore
                    if sum_dif < threshold:
                        threshold = sum_dif
                        ideal = i
                        df_4I[ideal] = df_ideal[ideal]
                    else:
                        pass
                else:
                    pass

        return df_4I

    def ideal_2_test(self, df_1, df_2, table_name_update, column_1_update, column_2_update):

        """
        With this function the best ideal function will be allocated to the test data and sql_database will be updated
        :param df_1: Dataframe that need to be tested e.g. "df_test"
        :param df_2: Dataframe to be checked against d.g. "df_ideal_4f"
        :param table_name_update: This table will be updated e.g. 'test'
        :param column_1_update: name of column to be updated e.g. 'Delta_Y'
        :param column_2_update: name of the column to be updated e.g. 'Nummer_der_Idealen_Funktion'
        :return: updated sql 'test' table with ideal function and Y delta values
        """

        # Determine the amount of lines for the interation
        row_count = len(df_1.index)

        # Check every line in the test data
        for f in range(row_count):
            row = df_test.iloc[f]
            x = row["x"]
            y = row["y"]
            id_column = df_2.loc[df_2['x'] == x]
            threshold = 100

            # Iterate to all 4 ideal function to find the best with difference smaller than sqrt2
            for i in ideal_4[1:]:
                id_y = id_column[i]
                dif = abs(id_y - y)
                dif_ = float(dif.to_string(index=False))
                if dif_ < (math.sqrt(2)):
                    if dif_ < threshold:
                        threshold = dif_

                        # update difference in SQL test date column "Delta_Y"
                        SqlData().update_values(table_name=table_name_update,
                                                column_value_x=x, column_value_y=y,
                                                column_name_entry=column_1_update, value=dif_)

                        # update ideal function name in SQL test date column "Nummer_der_Idealen_Funktion"
                        SqlData().update_values(table_name=table_name_update,
                                                column_value_x=x, column_value_y=y,
                                                column_name_entry=column_2_update, value=i[1:])
                    else:
                        pass

        print("Ideal functions have been allocated and written to 'test' sqlite table")

    def list_string_in_int(self, string_list):

        int_list = []
        for x in string_list[1:]:
            number = int(''.join(x)[1:])
            int_list.append(number)

        return int_list


# Identify the 4 ideal functions and put them in a list
df_ideal_4f = Compute().get_4_ideal_functions(df_1=df_train, df_2=df_ideal)
ideal_4 = list(df_ideal_4f.columns)
ideal_4_int = Compute().list_string_in_int(string_list=ideal_4)
print(f"Ideal functions for train functions identified:\n"
      f" Train 1 = Ideal {ideal_4_int[0]}\n"
      f" Train 2 = Ideal {ideal_4_int[1]}\n"
      f" Train 3 = Ideal {ideal_4_int[2]}\n"
      f" Train 4 = Ideal {ideal_4_int[3]}")

# Create new SQL Table with the 4 ideal functions
SqlData().create_sql_table_from_dataframe(dataframe=df_ideal_4f, sql_table_name="ideal_4")

# Allocate ideal function and delta value to test data and update 'test' sql table and 'test' dataframe
Compute().ideal_2_test(df_1=df_test, df_2=df_ideal_4f, table_name_update="test",
                       column_1_update="Delta_Y", column_2_update="Nummer_der_Idealen_Funktion")
df_test = SqlData().pd_from_sql(table_name="test")

"""
With the following code we will visualize the 4 'train' functions as well as the 4 'ideal'
functions including there identify test data
"""


class Visualize:
    """
    With this class we visualize proved data.
    """

    def __init__(self):

        # Style
        # plt.style.use('dark_background')

        # Create plot figure with two Subplot (axes)
        self.fig, (self.ax1, self.ax2) = plt.subplots(ncols=2, figsize=(15, 8))
        self.fig.suptitle("Result 'Hausarbeit' programming with python", fontsize=16)

        # Add grid to Axes
        self.ax1.grid(True, color="k")
        self.ax2.grid(True, color="k")

        # Aces label of the whole Figure
        self.fig.supylabel("y axis")
        self.fig.supxlabel("x axis")

        # Set Axes titles
        self.ax1.set_title("Trainings Data")
        self.ax2.set_title("Ideal Functions & Test Data")


class Axes(Visualize):

    def create_axes(self):
        # Line diagrams for  "train" functions on Axes 1
        self.ax1.plot(df_train[["x"]].to_numpy(),
                      df_train[["y1", "y2", "y3", "y4"]].to_numpy(),
                      label=["Train 1", "Train 2", "Train 3", "Train 4"], linewidth=2)

        # line diagrams for the identified 4 "ideal" functions on Axes_2
        self.ax2.plot(df_ideal_4f[["x"]].to_numpy(),
                      df_ideal_4f[ideal_4[1:]].to_numpy(),
                      label=[f"Ideal {ideal_4[1]}", f"Ideal {ideal_4[2]}",
                             f"Ideal {ideal_4[3]}", f"Ideal {ideal_4[4]}"], linewidth=2)

        # scattered diagrams for every entry of "test" that has been allocated one ideal functions
        self.ax2.scatter(df_test["x"].where(df_test["Nummer_der_Idealen_Funktion"] == ideal_4[1][1:]),
                         df_test["y"].where(df_test["Nummer_der_Idealen_Funktion"] == ideal_4[1][1:]),
                         label="Test1", s=20, color="blue")
        self.ax2.scatter(df_test["x"].where(df_test["Nummer_der_Idealen_Funktion"] == ideal_4[2][1:]),
                         df_test["y"].where(df_test["Nummer_der_Idealen_Funktion"] == ideal_4[2][1:]),
                         label="Test2", s=20, color="orange")
        self.ax2.scatter(df_test["x"].where(df_test["Nummer_der_Idealen_Funktion"] == ideal_4[3][1:]),
                         df_test["y"].where(df_test["Nummer_der_Idealen_Funktion"] == ideal_4[3][1:]),
                         label="Test3", s=20, color="green")
        self.ax2.scatter(df_test["x"].where(df_test["Nummer_der_Idealen_Funktion"] == ideal_4[4][1:]),
                         df_test["y"].where(df_test["Nummer_der_Idealen_Funktion"] == ideal_4[4][1:]),
                         label="Test4", s=20, color="red")

        # scattered diagrams for every entry of "test" has not been allocated to an ideal functions
        self.ax2.scatter(df_test["x"].where(df_test["Nummer_der_Idealen_Funktion"].isnull()),
                         df_test["y"].where(df_test["Delta_Y"].isnull()),
                         label="Out", s=15, marker="x", color="grey")

        # Legend added to both Axes
        self.ax1.legend()
        self.ax2.legend()

        return plt.show()


Axes().create_axes()
