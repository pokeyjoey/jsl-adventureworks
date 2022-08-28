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


#print('')
#print('')
#print('Read the SalesOrderHeader table')
#sql = "select * from sales.SalesOrderHeader limit 2"
#print(pd.read_sql(sql, conn).T)

print('')
print('')
print('Find earliest date of orders in the SalesOrderHeader table')
sql = """SELECT orderdate FROM sales.SalesOrderHeader
            ORDER BY orderdate ASC
            LIMIT 1 """
print(pd.read_sql(sql, conn).T)

print('')
print('')
print('Find latest date of orders in the SalesOrderHeader table')
sql = """SELECT orderdate FROM sales.SalesOrderHeader
            ORDER BY orderdate DESC
            LIMIT 1 """
print(pd.read_sql(sql, conn).T)

print('')
print('')
print('Find top five total amounts spent by customers since Junly, 1, 2013 returning cutomeruds and total amounts spent in the SalesOrderHeader table')
sql = """SELECT customerid, SUM(totaldue) AS total_spent
            FROM sales.SalesOrderHeader s
            WHERE orderdate > '2013-07-01'
            GROUP BY customerid
            ORDER BY total_spent DESC
            LIMIT 5"""
print(pd.read_sql(sql, conn))

print('')
print('')
print('Read the SalesOrderHeader table')
sql = "select * from sales.SalesOrderHeader limit 2"
print(pd.read_sql(sql, conn).T)

print('')
print('')
print('Read the SalesOrderDetail table')
sql = "select * from sales.SalesOrderDetail limit 2"
print(pd.read_sql(sql, conn).T)

print('')
print('')
print('Read the SalesTerritory table')
sql = "select * from sales.SalesTerritory limit 2"
print(pd.read_sql(sql, conn).T)

print('')
print('')
print('Find the names of the top five products brought in most revenue and the total amounts of revenue since Junly, 1, 2013 in the SalesOrderHeader, SalesOrderDetail, and Product tables')
sql = """SELECT p.name, SUM(sod.unitprice) AS total_revenue
            FROM sales.SalesOrderDetail sod
            JOIN production.product p
            ON sod.productid = p.productid
            WHERE sod.modifieddate > '2013-07-01'
            GROUP BY p.name
            ORDER BY total_revenue DESC
            LIMIT 5"""
print(pd.read_sql(sql, conn))

print('')
print('')
print('Find the top ten sales territories with the most sales in the SalesOrderHeader, SalesOrderDetail, and Product tables')
sql = """SELECT st.name, SUM(soh.totaldue) AS total_revenue
            FROM sales.SalesOrderHeader soh
            JOIN sales.SalesTerritory st
            ON soh.territoryid = st.territoryid
            WHERE soh.orderdate > '2013-07-01'
            GROUP BY st.name
            ORDER BY total_revenue DESC
            LIMIT 10"""
print(pd.read_sql(sql, conn))

print('')
print('')
print('select from the sales.SalesPerson table')
sql = "select * from sales.SalesPerson limit 2"
print(pd.read_sql(sql, conn).T)

print('')
print('')
print('Read the SalesTerritory table')
sql = "select * from sales.SalesTerritory limit 2"
print(pd.read_sql(sql, conn).T)

print('')
print('')
print('Find the salesperson id fo the salesperson who had the most sales in the Southwest Territory in the SalesPerson and the SalesTerritory tables')
sql = """SELECT sp.businessentityid, sp.salesytd
            FROM sales.SalesPerson sp
            ORDER BY sp.salesytd DESC
            LIMIT 1"""
print(pd.read_sql(sql, conn))


print('')
print('')
print('Top three items sold by sales person 276 in the SalesOrderHeader, SalesOrderDetail, and Product tables')
sql = """SELECT p.name, COUNT(*) AS items_sold
            FROM sales.SalesOrderHeader soh
            JOIN sales.SalesOrderDetail sod
            ON soh.salesorderid = sod.salesorderid
            JOIN production.product p
            ON sod.productid = p.productid
            WHERE soh.salespersonid =  276
            GROUP BY p.name
            ORDER BY items_sold DESC
            LIMIT 3"""
print(pd.read_sql(sql, conn))

