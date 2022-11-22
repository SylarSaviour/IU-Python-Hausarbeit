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
import matplotlib.pyplot as plt

"""
GET csv data into python objects. Datasetes should be stored in program folder.
"""

# Reading data from csv files using pandas
data_train = pd.read_csv('Datensatz/train.csv')
data_test = pd.read_csv('Datensatz/test.csv')
data_ideal = pd.read_csv('Datensatz/ideal.csv')

"""
CREATE an sqlite database in project folder /
With the following code we connect th program to the database and create sqlite tables /
from data_train and data_ideal based on csv files previously loaded
"""

# Connect an SQLite Database
con_str = "sqlite:///Hausarbeit.db"  # link to created database
engine = db.create_engine(con_str)
connection = engine.connect()

# Create Tables based on CSV files
def create_sql(csv_data, sql_tablename):

    """
    Enter the 'csv_data' object and the 'sql_tablename' to transform the one in the other
    The exception handling ensure that the program gets executed although a table with entered name already exists.
    """

    try:
        csv_data.to_sql(sql_tablename, engine, index=False) == ValueError
        pass
    except:
        print(f"sql table with name '{sql_tablename}' already exist and will be not created again")
        pass
    finally:
        pass

# Create sqlite database by entering existing data object and to be created table name as string
create_sql(data_train, "train")
create_sql(data_ideal, "ideal")

"""
COMPUTE_1: identify the four ideal function from data_ideal that fit best to data_train / 
where the deviations are less than sqrt(2)
"""

# idealfunc_1 = none
# idealfunc_2 = none
# idealfunc_3 = none
# idealfunc_4 = none

