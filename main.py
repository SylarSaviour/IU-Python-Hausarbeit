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
import numpy as np
import math
import sqlalchemy as db
import sqlite3
import matplotlib.pyplot as plt

"""
GET csv data into python objects. Datasetes should be stored in program folder.
"""

"""
With the following code we create an sqlite database in project folder /
connect th program to the database and create sqlite tables /

! ! ! ATTENTION ! ! !
Please store the "Datensatz" folder with the provides 3 cvs files "train", "ideal" and "test" in the project folder.
"""

class SQL_Data():

	"""
	This class controlles the creation, access and update of sqlite tables
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
	def create_sql_table_from_csv(self, csv_path, sql_tablename):
		"""
		Enter the 'csv_path'(string) object and the new 'sql_tablename'(string) to transform the one into the other
		The exception handling ensure that the program gets executed although a table with entered name already exists.
		"""
		csv_data = pd.read_csv(f'{csv_path}')
		try:
			csv_data.to_sql(sql_tablename, self.engine, index=False, if_exists="fail")
			print(f"tabel '{sql_tablename}' has been created")
			pass
		except:
			print(f"SQL table '{sql_tablename}' already exist")
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
			print(f"new column '{new_column_name}' in table '{table_name}' already exist")
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
		self.cur.execute(f"SELECT * FROM {table_name} WHERE {column_name_x}={column_value_x}")
		result = self.cur.fetchall()
		if not result:
			raise ValueError("no matching criteria to update values in db")
		else:
			self.cur.execute(f"UPDATE {table_name} SET {column_name_entry}={value} WHERE {column_name_x}={column_value_x} AND {column_name_y}={column_value_y}")
			self.con.commit()
			print("Data Update")
			
			
	def select_values(self, table_name, column_name, value):
	
		"""
		with this funktion data from a databse can be selected
		:param column_name:
		:return:
		"""
		self.cur.execute(f"SELECT * FROM {table_name} WHERE {column_name}={value}")
		result = self.cur.fetchall()
		
	def select_column(self, table_name, column_name):
		self.cur.execute(f"SELECT {column_name} FROM {table_name}")
		result = self.cur.fetchall()
		list =[]
		
		for row in result:
			list += row
			
		return(list)
		
	def select_table_values(self, table_name):
		"""
		with this funktion data from a databse can be selected
		:param column_name:
		:return:
		"""
		self.cur.execute(f"SELECT * FROM {table_name}")
		result = self.cur.fetchall()
		
		for row in result:
			print(row)
			
	def pd_from_csv(self, csv_path):
		df = pd.read_csv(csv_path)
		return(df)
		
# Create sqlite database by entering existing data object and to be created table name as string
data = SQL_Data()
data.create_sql_table_from_csv(csv_path="Datensatz/train.csv", sql_tablename="train")
data.create_sql_table_from_csv(csv_path="Datensatz/ideal.csv", sql_tablename="ideal")
data.create_sql_table_from_csv(csv_path="Datensatz/test.csv", sql_tablename="test")

# Add new column to test table that did not exist before
data.add_table_column(table_name="test", new_column_name="Delta_Y")
data.add_table_column(table_name="test", new_column_name="Nummer_der_Idealen_Funktion")


"""
Some TESTING Code for Database interactions
"""
# Test the data update function
# data.update_values(table_name="test",
#                    column_name_entry="Nummer_der_Idealen_Funktion",
#                    value=1,
#                    column_value_x=-300,
#                    column_value_y=34.25082)

#Test data selection
# data.select_values(table_name="test", column_name="Nummer_der_Idealen_Funktion", value=1)
# data.select_table_values(table_name="test")

"""
With the following code we will identify the 4 ideal function and save them in a new dataset
"""

class Compute(SQL_Data):

	df_ideal = SQL_Data().pd_from_csv("Datensatz/ideal.csv")
	df_train = SQL_Data().pd_from_csv("Datensatz/train.csv")
	df_4I = pd.DataFrame()
	df_4I["x"] = df_train["x"]

	functions_train = list(df_train.columns)
	functions_ideal = list(df_ideal.columns)

	def get_ideal_functions(self,):

		for t in self.functions_train[1:]:
			min = 10
			ideal = None	
		
			for i in self.functions_ideal[1:]:
				function_train = t
				function_check = i

				self.df_train['check'] = np.where((self.df_train[function_train] - self.df_ideal[function_check] <= math.sqrt(2)/2) &
				(self.df_train[function_train] - self.df_ideal[function_check] >= (math.sqrt(2)/2)*-1), 'True', 'False')
				self.df_train['dif'] = self.df_train[function_train] - self.df_ideal[function_check]
				
				result = self.df_train[self.df_train['check'] == 'True'].count()
				sum_dif = self.df_train['dif'].sum()
				

				if result['check'] == 400:
					
					if sum_dif < min:
						min = sum_dif 
						ideal = i
						self.df_4I[ideal] = self.df_ideal[ideal]
					else:
						pass
				else:
					pass
				
		print(self.df_4I)

go = Compute().get_ideal_functions()
