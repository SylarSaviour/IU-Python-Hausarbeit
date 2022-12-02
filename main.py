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
- Use of libraries like pandas, matplotlib etc.
- Unite test wherever this is suitable
- Full program documentation using docstrings
- Use Guit for version control
"""

"""
With the following code we create an sqlite database in project folder /
connect th program to the database and create sqlite tables /

! ! ! ATTENTION ! ! !
Please store the "Datensatz" folder with the provides 3 cvs files "train", "ideal" and "test" in the project folder.
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

    def select_column_names(self, table_name):

        """
        with this function specific data from a database can be selected
        :param table_name: name of the sql table as string
        :return: list with column names
        """
        cursor = self.con.execute(f'select * from {table_name}')
        names = [description[0] for description in cursor.description]

        return names

    def select_values(self, table_name, column_name, value):

        """
        with this function specific data from a database can be selected
        :param table_name: name of the sql table as string
        :param column_name: name of column condition as string
        :param value: value inside the column condition
        :return: ???????????????????????????????????????????????????
        """

        self.cur.execute(f"SELECT * FROM {table_name} WHERE {column_name}={value}")
        self.cur.fetchall()

    def select_one_column(self, table_name, column_name):

        """
        with this function a single column from a database can be selected
        :param table_name: name of the sql table as string
        :param column_name: name of column condition as string
        :return: list of values of selected column
        """

        self.cur.execute(f"SELECT {column_name} FROM {table_name}")
        result = self.cur.fetchall()
        c_list = []

        for row in result:
            c_list += row

        return c_list

    def select_one_column_cond(self, table_name,
                               column_name,
                               column_name_condition="?", column_value_condition="?"):

        """
        with this function two columns from a database can be selected with a condition statement
        :param table_name: name of the sql table as string
        :param column_name: name of first column to be returned as string
        :param column_name_condition: column name of condition as string
        :param column_value_condition: column value as condition for selection
        :return: list of values of selected column
        """

        self.cur.execute(f"SELECT {column_name} "
                         f"FROM {table_name} "
                         f"WHERE {column_name_condition}={column_value_condition}")

        return self.cur.fetchall()

    def select_one_column_cond_null(self, table_name, column_name, isnull, column_name_condition="?"):

        """
        with this function two columns from a database can be selected with a condition statement
        :param table_name: name of the sql table as string
        :param column_name: name of first column to be returned as string
        :param isnull: requires 'true' or 'false' input to determine if Null values should be shown or not
        :param column_name_condition: column name of condition as string
        :return: list of NULL values of selected column
        """

        if isnull == "true":
            self.cur.execute(f"SELECT {column_name} "
                             f"FROM {table_name} "
                             f"WHERE {column_name_condition} IS NULL")
        elif isnull == "false":
            self.cur.execute(f"SELECT {column_name} "
                             f"FROM {table_name} "
                             f"WHERE {column_name_condition} IS NOT NULL")
        else:
            raise ValueError("Wrong Argument: Must be 'true' or 'false'")

        return self.cur.fetchall()

    def select_two_column(self, table_name,
                          column_name_1, column_name_2):

        """
        with this function two columns from a database can be selected
        :param table_name: name of the sql table as string
        :param column_name_1: name of first column to be returned as string
        :param column_name_2: name of second column to be returned as string
        :return: list of values of selected column
        """

        self.cur.execute(f"SELECT {column_name_1}, {column_name_2} "
                             f"FROM {table_name}")

        return self.cur.fetchall()

    def select_two_column_cond(self, table_name,
                               column_name_1, column_name_2,
                               column_name_condition="?", column_value_condition="?"):

        """
        with this function two columns from a database can be selected with a condition statement
        :param table_name: name of the sql table as string
        :param column_name_1: name of first column to be returned as string
        :param column_name_2: name of second column to be returned as string
        :param column_name_condition: column name of condition as string
        :param column_value_condition: column value as condition for selection
        :return: list of values of selected column
        """

        self.cur.execute(f"SELECT {column_name_1}, "
                         f"{column_name_2} FROM {table_name} "
                         f"WHERE {column_name_condition}={column_value_condition}")

        return self.cur.fetchall()

    def select_table_values(self, table_name):

        """
        with this function data from an entire sql table can be selected
        :param table_name: name of the table
        :return: ?????????????????????????????????????????????????????
        """

        self.cur.execute(f"SELECT * FROM {table_name}")
        return self.cur.fetchall()

    def select_table_rows(self, table_name):

        """
        with this function data from an entire sql table can be selected
        :param table_name: name of the table
        :return: ?????????????????????????????????????????????????????
        """

        self.cur.execute(f"SELECT * FROM {table_name}")
        return len(self.cur.fetchall())

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

# Add new column to test table that did not exist before
data.add_table_column(table_name="test", new_column_name="Delta_Y", column_type="REAL")
data.add_table_column(table_name="test", new_column_name="Nummer_der_Idealen_Funktion", column_type="TEXT")


class Compute(SqlData):
    """
    With this class we identify the 4 ideal functions and allocate the best ideal function to the test dataset
    """

    # turn SQL tables in dataframes for further calculations
    df_ideal = SqlData().pd_from_sql(table_name="ideal")
    df_train = SqlData().pd_from_sql(table_name="train")
    df_test = SqlData().pd_from_sql(table_name="test")
    df_4I = pd.DataFrame()
    df_4I["x"] = df_train["x"]

    functions_train = list(df_train.columns)
    functions_ideal = list(df_ideal.columns)

    # df_ideal_help = pd.merge(df_test, df_ideal)
    # df_ideal_help = df_ideal_help.drop(columns=["y", "Delta_Y", "Nummer_der_Idealen_Funktion"], axis=1)
    # function_ideal_help = list(df_ideal_help.columns)

    def get_4_ideal_functions(self):

        """
        with this function the 4 ideal functions from ideal.csv are identified
        :param ?????????????????????????????????????????????????????
        :return: pandas dataframe 'df_4I' with 4 ideal functions
        """

        # Get number of columns to iterate through
        row_count = SqlData().select_table_rows(table_name="train")

        for t in self.functions_train[1:]:
            threshold = 10

            for i in self.functions_ideal[1:]:
                function_train = t
                function_check = i

                self.df_train['check'] = np.where((self.df_train[function_train] - self.df_ideal[function_check]
                                                   <= math.sqrt(2)) &
                                                  (self.df_train[function_train] - self.df_ideal[function_check]
                                                   >= (math.sqrt(2)) * -1),
                                                  'True', 'False')

                self.df_train['dif'] = self.df_train[function_train] - self.df_ideal[function_check]

                result = self.df_train[self.df_train['check'] == 'True'].count()
                sum_dif = abs(self.df_train['dif'].sum())

                if result['check'] == row_count:

                    if sum_dif < threshold:
                        threshold = sum_dif
                        ideal = i
                        self.df_4I[ideal] = self.df_ideal[ideal]
                    else:
                        pass
                else:
                    pass
        return self.df_4I

    def ideal_2_test(self):

        """
        with this function the best ideal function will be  allocated to the test sql database
        :param ?????????????????????????????????????????????????????
        :return: updated sql 'test' table with ideal function and Y delta values
        """

        df_ideal_help = pd.merge(self.df_test, df_ideal_4f)
        df_ideal_help = df_ideal_help.drop(columns=["y", "Delta_Y", "Nummer_der_Idealen_Funktion"], axis=1)
        function_ideal_help = list(df_ideal_help.columns)
        row_count = SqlData().select_table_rows(table_name="test")

        count = 0
        for f in range(row_count):
            row = self.df_test.iloc[f]
            x = row["x"]
            y = row["y"]
            id_column = df_ideal_4f.loc[df_ideal_4f['x'] == x]
            threshold = 1

            for i in function_ideal_help[1:]:
                id_y = id_column[i]
                dif = abs(id_y - y)
                dif_ = float(dif.to_string(index=False))
                if dif_ < (math.sqrt(2)):
                    if dif_ < threshold:
                        count += 1
                        threshold = dif_

                        # update difference in SQL test date column "Delta_Y"
                        SqlData().update_values(table_name="test",
                                                column_value_x=x, column_value_y=y,
                                                column_name_entry="Delta_Y", value=dif_)

                        # update ideal function name in SQL test date column "Nummer_der_Idealen_Funktion"
                        SqlData().update_values(table_name="test",
                                                column_value_x=x, column_value_y=y,
                                                column_name_entry="Nummer_der_Idealen_Funktion", value=i[1:])
                    else:
                        pass
        print(f"Done {count} test functions allocated to one of the 4 ideal functions"
              f"{row_count-count} test functions out of range (sqr2)")

# Identify the 4 ideal functions
df_ideal_4f = Compute().get_4_ideal_functions()

# Create new SQL Table with 4 ideal functions
ideal_SQL = SqlData().create_sql_table_from_dataframe(dataframe=df_ideal_4f, sql_table_name="ideal_4")

# list of the f ideal functions
ideal_f = SqlData().select_column_names(table_name="ideal_4")
# Allocate ideal function and delta value to test data and update sql table
comp = Compute().ideal_2_test()


"""
VISUALIZATION
"""

# test function 1 x/y values
train_x = SqlData().select_one_column(table_name="train", column_name="x")
train_y1 = SqlData().select_one_column(table_name="train", column_name="y1")
train_y2 = SqlData().select_one_column(table_name="train", column_name="y2")
train_y3 = SqlData().select_one_column(table_name="train", column_name="y3")
train_y4 = SqlData().select_one_column(table_name="train", column_name="y4")

# ideal function 1 x/y values
ideal_x = SqlData().select_one_column(table_name="ideal_4", column_name="x")
ideal_y1 = SqlData().select_one_column(table_name="ideal_4", column_name=f"{''.join(ideal_f[1:2])}")
ideal_y2 = SqlData().select_one_column(table_name="ideal_4", column_name=f"{''.join(ideal_f[2:3])}")
ideal_y3 = SqlData().select_one_column(table_name="ideal_4", column_name=f"{''.join(ideal_f[3:4])}")
ideal_y4 = SqlData().select_one_column(table_name="ideal_4", column_name=f"{''.join(ideal_f[4:5])}")

# test function 1 x/y values
test_x_ok = SqlData().select_one_column_cond_null(table_name="test", column_name="x",
                                                  column_name_condition="Nummer_der_Idealen_Funktion",
                                                  isnull="false")
test_y_ok = SqlData().select_one_column_cond_null(table_name="test", column_name="y",
                                                  column_name_condition="Nummer_der_Idealen_Funktion",
                                                  isnull="false",)

test_x_not_ok = SqlData().select_one_column_cond_null(table_name="test", column_name="x",
                                                      column_name_condition="Nummer_der_Idealen_Funktion",
                                                      isnull="true")
test_y_not_ok = SqlData().select_one_column_cond_null(table_name="test", column_name="y",
                                                      column_name_condition="Nummer_der_Idealen_Funktion",
                                                      isnull="true",)


# Style
# style.use('ggplot')

# Plot figure with two Subplot (axes) erzeugen
fig, (ax1, ax2) = plt.subplots(ncols=2, figsize=(15, 8))
fig.suptitle("Result 'Hausarbeit' programming with python", fontsize=16)


# Lineplot erzeugen
ax1.plot(train_x, train_y1, label="Train 1", linewidth=1)
ax1.plot(train_x, train_y2, label="Train 2", linewidth=1)
ax1.plot(train_x, train_y3, label="Train 3", linewidth=1)
ax1.plot(train_x, train_y4, label="Train 4", linewidth=1)

ax2.plot(train_x, ideal_y1, label=f"{''.join(ideal_f[1:2])}", linewidth=1)
ax2.plot(train_x, ideal_y2, label=f"{''.join(ideal_f[2:3])}", linewidth=1)
ax2.plot(train_x, ideal_y3, label=f"{''.join(ideal_f[2:3])}", linewidth=1)
ax2.plot(train_x, ideal_y4, label=f"{''.join(ideal_f[4:5])}", linewidth=1)

ax2.scatter(test_x_ok, test_y_ok, label="Test OK", s=20, color="green")
ax2.scatter(test_x_not_ok, test_y_not_ok, label="Test not-OK", s=15, marker="x", color="grey")

# Legende hinzufügen
ax1.legend()
ax2.legend()

# Grid hinzufügen
ax1.grid(True, color="k")
ax2.grid(True, color="k")


# Achsen beschriften
fig.supylabel("y axis")
fig.supxlabel("x axis")

# Set Axes Titel
ax1.set_title("Trainings Data")
ax2.set_title("Ideal Functions & Test Data")

# Plot anzeigen
plt.show()
