import pandas as pd
import psycopg2
import warnings

warnings.filterwarnings('ignore')

conn = psycopg2.connect(database="adventureworks", user="postgres")
cursor = conn.cursor()

print(" select one record from the database")
sql = """select * from person.address limit 1"""
cursor.execute(sql)
print(cursor.fetchall())

print('')
print('')
print('select from the sales schema')
sql = "select * from sales.Customer limit 5"
cursor.execute(sql)
print(cursor.fetchall())


print('')
print('')
print('Read the SalesOrderHeader table')
sql = "select * from sales.SalesOrderHeader limit 2"
print(pd.read_sql(sql, conn).T)
