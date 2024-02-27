# Databricks notebook source
from pyspark.sql.functions import col,lit,desc,to_date,lag,lead,avg,sum,unix_timestamp,rank,dense_rank,row_number,substring,split,explode,expr,count,to_timestamp,when,max,min,date_diff,date_format,day,countDistinct,from_json,schema_of_json,round,asc,countDistinct

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

from pyspark import Row

# COMMAND ----------

data = [
   ('Jan',2000,1500,3000),
   ('Feb',1000,2500,4000),
   ('Mar',2000,1400,1000),
   ('Apr',3000,1500,1000)
]

# COMMAND ----------

df = spark.createDataFrame(data,"month string,clothing int,electronics int,sports int")
df.display()

# COMMAND ----------

window  = Window.orderBy(col("TotSales").desc())

# COMMAND ----------

df.groupBy(col("month")) \
  .agg((sum(col("clothing") + col("electronics") + col("sports")).alias("TotSales"))) \
  .withColumn("SalesRank",dense_rank().over(window)) \
  .filter(col("SalesRank") == 1).display()

# COMMAND ----------

delivery_data = [
    (1, 1, "2019-08-01", "2019-08-02"),
    (2, 2, "2019-08-02", "2019-08-02"),
    (3, 1, "2019-08-11", "2019-08-12"),
    (4, 3, "2019-08-24", "2019-08-24"),
    (5, 3, "2019-08-21", "2019-08-22"),
    (6, 2, "2019-08-11", "2019-08-13"),
    (7, 4, "2019-08-09", "2019-08-09")
]

delivery_schema = "delivery_id int, customer_id int, order_date string, customer_pref_delivery_date string"

# COMMAND ----------

df = spark.createDataFrame(delivery_data,delivery_schema)

# COMMAND ----------

df = df.withColumn("order_date",col("order_date").cast("date")) \
       .withColumn("customer_pref_delivery_date",col("customer_pref_delivery_date").cast("date"))
df.display()

# COMMAND ----------

window = Window.partitionBy(col("customer_id")).orderBy(col("order_date"))

# COMMAND ----------

df.withColumn("customerorders",dense_rank().over(window)) \
  .filter(col("customerorders") == 1) \
  .withColumn("sameday",when(col("order_date") == col("customer_pref_delivery_date"),1)) \
  .agg(sum(col("customerorders")).alias("customorders"),sum(col("sameday")).alias("sameorders")) \
  .withColumn("percent",round((col("sameorders").cast("float") / col("customorders").cast("float"))*100,1)).display()

# COMMAND ----------

data = [
    (1, 'Math', 90),
    (1, 'Science', 93),
    (1, 'History', 85),
    (2, 'Math', 85),
    (2, 'Science', 79),
    (2, 'History', 96),
    (3, 'Math', 95),
    (3, 'Science', 87),
    (3, 'History', 77),
    (4, 'Math', 78),
    (4, 'Science', 91),
    (4, 'History', 90),
    (5, 'Math', 92),
    (5, 'Science', 84),
    (5, 'History', 88),
]

# COMMAND ----------

df = spark.createDataFrame(data,"studentid int,subject string,marks int")
df.display()

# COMMAND ----------

window = Window.partitionBy(col("subject")).orderBy(col("marks").desc())

# COMMAND ----------

df.withColumn("subjectrank",dense_rank().over(window)) \
  .filter(col("subjectrank") == 1) \
  .select(col("studentid"),col("subject"),col("marks")).display()

# COMMAND ----------


employee_projects_data = [
    (1, 'Project1', '2022-01-10'),
    (1, 'Project2', '2022-02-15'),
    (1, 'Project3', '2022-03-20'),
    (2, 'Project1', '2022-01-05'),
    (2, 'Project2', '2022-02-10'),
    (2, 'Project3', '2022-03-15'),
    (2, 'Project4', '2022-04-20')
]

# COMMAND ----------

df = spark.createDataFrame(employee_projects_data,"employee_id int ,project_name string, project_date string")
df.display()

# COMMAND ----------

df.withColumn("project_date",col("project_date").cast("date")) \
  .groupBy(col("employee_id")) \
  .agg(countDistinct(col("project_name")).alias("ProjectsCount"),max(col("project_date")).alias("LatestProject")) \
  .display()

# COMMAND ----------

prescriptions_data = [
    (1, 1, 1),
    (2, 2, 1),
    (3, 3, 1),
    (4, 1, 2),
    (5, 2, 2),
    (6, 3, 2),
    (7, 1, 3),
    (8, 2, 4),
    (9, 3, 4),
    (10, 4, 5),
    (11, 5, 5),
    (12, 6, 5)
]

# COMMAND ----------

df = spark.createDataFrame(prescriptions_data,"pres_id int,doctor_id int,medication_id int")
df.display()

# COMMAND ----------

df.groupBy(col("medication_id")) \
  .agg(countDistinct(col("doctor_id")).alias("doctorcount")) \
  .filter(col("doctorcount") >= 3) \
  .display()

# COMMAND ----------

airlines_data = [
    (1, "Airline A"),
    (2, "Airline B"),
    (3, "Airline C"),
]

# COMMAND ----------

flights_data = [
    (1, 1, 101),  
    (2, 1, 102),  
    (3, 2, 101), 
    (4, 2, 103),
    (5, 3, 101),  
    (6, 3, 102), 
    (7, 3, 103) 
]

# COMMAND ----------

df = spark.createDataFrame(flights_data,"flight_id int,airline_id int,destination_airport_id int")
df.display()

# COMMAND ----------

df2 = df.select(col("destination_airport_id")).distinct().count()
df2

# COMMAND ----------

df.groupBy("airline_id") \
  .agg(countDistinct(col("destination_airport_id")).alias("airportscount")) \
  .filter(col("airportscount") == df2) \
  .display()

# COMMAND ----------

data = [
    ("c1","New York","Lima"),
    ("c1","London","New York"),
    ("c1","Lima","Sao Paulo"),
    ("c1","Sao Paulo","New Delhi"),
    ("c2","Mumbai","Hyderabad"),
    ("c2","Surat","Pune"),
    ("c2","Hyderabad","Surat"),
    ("c3","Kochi","Kurnool"),
    ("c3","Lucknow","Agra"),
    ("c3","Agra","Jaipur"),
    ("c3","Jaipur","Kochi")
]

# COMMAND ----------

df = spark.createDataFrame(data,"customer string,starting_loc string,ending_loc string")
df.display()

# COMMAND ----------

df.alias("df").join(df.alias("df1"),col("df.starting_loc") == col("df1.ending_loc"),"left") \
  .join(df.alias("df2"),col("df.ending_loc") == col("df2.starting_loc"),"left") \
  .groupBy(col("df.customer")) \
  .agg(max(when(col("df1.starting_loc").isNull(),col("df.starting_loc"))).alias("startinglocation"),max(when(col("df2.ending_loc").isNull(),col("df.ending_loc"))).alias("endinglocation")) \
  .orderBy(asc("customer")) \
  .display()

# COMMAND ----------

data = [
    ("Siva",1,30000),
    ("Ravi",2,40000),
    ("Prasad",1,50000),
    ("Arun",1,30000),
    ("Sai",2,20000)
]

# COMMAND ----------

df = spark.createDataFrame(data,"name string,deptid int,salary int")
df.display()

# COMMAND ----------

window = Window.partitionBy("deptid").orderBy(desc("salary"),desc("name"))

# COMMAND ----------

df2 = df.withColumn("Rank",rank().over(window)) \
  .withColumn("count",count(col("name")).over(Window.partitionBy("deptid")))

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.groupBy(col("deptid")) \
     .agg(max(when(col("Rank") == 1,col("name"))).alias("empmaxsalary"),
          max(when(col("Rank") == col("count"),col("name"))).alias("empminsalary")) \
     .display()

# COMMAND ----------

df.withColumn("Rank",rank().over(window)) \
  .withColumn("count",count(col("name")).over(Window.partitionBy("deptid"))) \
  .display()

# COMMAND ----------

df.groupBy(col("deptid")) \
  .agg(min(col("Salary"))) \
  .display()

# COMMAND ----------

data = [
    (1,"Arjun Patel",85000,1),
    (2,"Aarav Sharma",80000,2),
    (3,"Zara Singh",60000,2),
    (4,"Priya Reddy",90000,1),
    (5,"Aditya Kapoor",69000,1),
    (6,"Naina Varma",85000,1),
    (7,"Kabir Gupta",70000,1)
]

# COMMAND ----------

dep = [
    (1,"IT"),
    (2,"Sales")
]

# COMMAND ----------

df2 = spark.createDataFrame(dep,"id int,department string")
df2.display()

# COMMAND ----------

df = spark.createDataFrame(data,"id int,name string,salary int,deptid int")
df.display()

# COMMAND ----------

df.orderBy(col("deptid").asc(),col("salary").desc()).display()

# COMMAND ----------

window = Window.partitionBy("department").orderBy(desc("salary"))

# COMMAND ----------

df.alias("df").join(df2.alias("d"),col("d.id") == col("df.deptid")) \
              .drop(col("df.deptid"),col("d.id")) \
              .withColumn("Rank",dense_rank().over(window)) \
              .filter(col("Rank") <= 3) \
              .select("department","name","salary") \
              .display()

# COMMAND ----------

data = [
    (11114,'8:30','I'),
    (11114,'10:30','O'),
    (11114,'11:30','I'),
    (11114,'15:30','O'),
    (11115,'9:30','I'),
    (11115,'17:30','O')
]

# COMMAND ----------

df = spark.createDataFrame(data,"empid int,swipe string,flag string")
df.display()

# COMMAND ----------

df.withColumn("swipe",col("swipe").cast("Timestamp")).display()

# COMMAND ----------

df.groupBy(col("empid")) \
  .agg(min(when(col("flag") == "I",col("swipe"))).alias("minswipe"),max(when(col("flag") == "O",col("swipe"))).alias("maxswipe")) \
  .withColumn("cloked_hrs",col("maxswipe") - col("minswipe")) \
  .display()

# COMMAND ----------

date_diff

# COMMAND ----------

data = [
    (1,'Abbot'),
    (2,'Doris'),
    (3,'Emerson'),
    (4,'Green'),
    (5,'Jeames')
]

# COMMAND ----------

df = spark.createDataFrame(data,"id int,name string")

# COMMAND ----------

window = Window.orderBy("id")

# COMMAND ----------

df.withColumn("newname",when((col("id") % 2 == 1),lead("name").over(window)) \
                        .when(col("id")% 1 == 0,lag("name").over(window))) \
                        .display()

# COMMAND ----------

data = [
    ("John Doe",'{"Street" : "123 main strt","City" : "Anytown"}'),
    ("Jane Smith",'{"Street" : "456 elm strt","City" : "othertown"}')
]

# COMMAND ----------

df = spark.createDataFrame(data,"name string,address string")
df.display()

# COMMAND ----------

schema = 'Street String,City String'

# COMMAND ----------

df.select("name",from_json("address",schema_of_json('{"Street" : "123 main strt","City" : "Anytown"}'))).printSchema()

# COMMAND ----------

df.select("name",from_json("address",schema).alias("newaddress")) \
  .select("name","newaddress.Street","newaddress.City").display()

# COMMAND ----------

help(from_json)

# COMMAND ----------

data = [
    ("O1",'2023-01-01',300,2),
    ("O2",'2022-01-01',200,5),
    ("O3",'2023-02-03',600,5),
]

# COMMAND ----------

df = spark.createDataFrame(data,"orderid string,orderdate string,productprice int,quantity int")
df.display()

# COMMAND ----------

df2 = df.withColumn("orderdate",to_date("orderdate"))
df2.display()

# COMMAND ----------

total_sales = df2.select(sum(col("productprice") * col("quantity")).alias("Total_Sales"))
total_sales.display()

# COMMAND ----------

df2.crossJoin(total_sales).display()

# COMMAND ----------

data = [Row(employee_id=1, employee_name="John", salary=160000, manager_id=3),
        Row(employee_id=2, employee_name="Susan", salary=155000, manager_id=3),
        Row(employee_id=3, employee_name="Michael", salary=80000, manager_id=5),
        Row(employee_id=4, employee_name="Linda", salary=72000, manager_id=5),
        Row(employee_id=5, employee_name="David", salary=95000, manager_id=6),
        Row(employee_id=6, employee_name="Sarah", salary=110000, manager_id=None)]

# COMMAND ----------

df = spark.createDataFrame(data)
df.display()

# COMMAND ----------

df.alias("df1").join(df.alias("df2"),col("df1.employee_id") == col("df2.manager_id")) \
               .filter(col("df2.salary") > col("df1.salary")) \
               .select("df2.employee_name") \
               .display()

# COMMAND ----------

data = [
    ("C1","01/01/2019","12/30/2018"), 
    ("C1","01/02/2019","01/25/2019"),   
    ("C1","01/02/2019","01/24/2019"),   
    ("C2","05/01/2019","06/01/2019"),
    ("C2","05/02/2019","02/02/2019"),
    ("C2","05/03/2019","07/03/2019")
    ]

# COMMAND ----------

df =spark.createDataFrame(data,"cusid string,duedate string,paymentdate string")
df.display()

# COMMAND ----------

data = [
("2023-10-24", 100.0),
("2023-10-24", 200.0),
("2023-10-25", 150.0),
("2023-10-25", 250.0),
("2023-10-26", 300.0),
]

# COMMAND ----------

df = spark.createDataFrame(data,"date string,amount float")
df.display()

# COMMAND ----------

df.groupBy(col("date")) \
  .agg(avg(col("amount")).alias("avgamount")) \
  .display()

# COMMAND ----------

df = spark.read.json("abfss://raw@storageadlsdatabricks.dfs.core.windows.net/drivers.json")
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select("*",col("name.forename").alias("forename"),col("name.surname").alias("surname")) \
  .drop("name") \
  .display()

# COMMAND ----------

spark.read.json("abfss://raw@storageadlsdatabricks.dfs.core.windows.net/qualifying/").display()

# COMMAND ----------

cust = [
    (1,5),
    (2,6),
    (3,5),
    (3,6),
    (1,6)
]

# COMMAND ----------

products = [
    (5,),
    (6,)
]

# COMMAND ----------

cust_df = spark.createDataFrame(cust,"cust_id int,prod_id int")
cust_df.display()

# COMMAND ----------

prod_df = spark.createDataFrame(products,"prod_id int")
prod_df.display()

# COMMAND ----------

numproducts = prod_df.distinct().count()
numproducts

# COMMAND ----------

cust_df.groupBy("cust_id") \
       .agg(countDistinct(col("prod_id")).alias("products")) \
       .filter(col("products") == numproducts) \
       .select("cust_id") \
       .display()

# COMMAND ----------

  transactions = [
  (101, '2023-05-01', 100.00),
  (101, '2023-05-02', 150.00),
  (101, '2023-05-03', 200.00),
  (102, '2023-05-01', 50.00),
  (102, '2023-05-03', 100.00),
  (102, '2023-05-04', 200.00),
  (105, '2023-05-01', 100.00),
  (105, '2023-05-02', 150.00),
  (105, '2023-05-03', 200.00),
  (105, '2023-05-04', 300.00),
  (105, '2023-05-05', 700.00),
  (105, '2023-05-06', 250.00),
  (105, '2023-05-07', 220.00),
  (105, '2023-05-14', 10.00),
  (105, '2023-05-15', 50.00),
  (105, '2023-05-16', 100.00),
  (105, '2023-05-17', 450.00),
  (105, '2023-05-18', 70.00),
  (106, '2023-05-21', 100.00),
  (106, '2023-05-22', 450.00),
  (106, '2023-05-23', 70.00)
  ]

# COMMAND ----------

df = spark.createDataFrame(transactions,"customer_id int,transaction_date string,amount double")
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("transaction_date",col("transaction_date").cast("date"))
df.printSchema()

# COMMAND ----------

window = Window.partitionBy(col("customer_id")).orderBy(col("transaction_date"))

# COMMAND ----------

count_window = Window.partitionBy(col("customer_id"),col("diff"))

# COMMAND ----------

df.withColumn("diff",day(col("transaction_date")) - row_number().over(window)) \
  .withColumn("cnt",count("*").over(count_window)) \
  .filter("cnt > 2") \
  .groupBy("customer_id","cnt") \
  .agg(min(col("transaction_date")).alias("mindate"),max(col("transaction_date")).alias("maxdate")) \
  .drop("cnt") \
  .display()

# COMMAND ----------

sales = [
('2023-09-01','TV',100),
('2023-09-01','Laptop',80),
('2023-09-02','TV',150),
('2023-09-02','Laptop',75),
('2023-09-03','TV',200),
('2023-09-03','Laptop',0),
('2023-09-04','TV',98),
('2023-09-04','Laptop',100)
]

# COMMAND ----------

df = spark.createDataFrame(sales,"sales_date string,electronics string,sold_items int")
df.display()

# COMMAND ----------

df.withColumn("laptop_sales",when(col("electronics") == 'Laptop',col("sold_items"))) \
  .withColumn("tv_sales",when(col("electronics") == 'TV',col("sold_items"))) \
  .groupBy("sales_date") \
  .agg((sum(col("tv_sales")) - sum(col("laptop_sales"))).alias("diff")) \
  .display()

# COMMAND ----------

players_table = [
(1 , '100m' , 2016 , 'Amthhew' , 'Donald' , 'Barbara'),
(2 , '200m' , 2016 , 'Nichole' , 'Alvaro' , 'Janet'),
(3 , '500m' , 2016 , 'Charles' , 'Nichole' , 'Susana'),
(4 , '100m' , 2016 , 'Ronald' , 'Maria' , 'Paula'),
(5 , '200m' , 2016 , 'Alfred' , 'Carol' , 'Steven'),
(6 , '500m' , 2016 , 'Nichole' , 'Alfred' , 'Brandon'),
(7 , '100m' , 2016 , 'Charles' , 'Dennis' , 'Susana'),
(8 , '200m' , 2016 , 'Thomas' , 'Dawn' , 'Catherine'),
(9 , '500m' , 2016 , 'Thomas' , 'Dennis' , 'Paula'),
(10 , '100m' , 2016 , 'Charles' , 'Dennis' , 'Susana'),
(11 , '200m' , 2016 , 'Jessica' , 'Donald' , 'Stefeney'),
(12 , '500m' , 2016 , 'Thomas' , 'Steven' , 'Catherine')
]

# COMMAND ----------

columns = "id int,sprint string,year int,gold string,silver string,bronze string"

# COMMAND ----------

df = spark.createDataFrame(players_table,columns)
df.display()

# COMMAND ----------

df.display()

# COMMAND ----------

df_silver = df.drop("gold","bronze")
df_silver.display()

# COMMAND ----------

df_bronze = df.drop("gold","silver")
df_bronze.display()

# COMMAND ----------

df3 = df_bronze.union(df_silver)
df3.display()

# COMMAND ----------

df.alias("df").join(df3.alias("df3"),col("df.gold") == col("df3.bronze"),"left") \
  .filter("df3.bronze is null") \
  .groupby("df.gold") \
  .count().display()

# COMMAND ----------

customer_orders = [
    (1,100,"2022-01-01",2000),
    (2,200,'2022-01-01' ,2500),
    (3,300,'2022-01-01',2100),
    (4,100,'2022-01-02',2000),
    (5,400,'2022-01-02',2200),
    (6,500,'2022-01-02',2700),
    (7,100,'2022-01-03',3000),
    (8,400,'2022-01-03',1000),
    (9,600,'2022-01-03',3000)
]

# COMMAND ----------

df = spark.createDataFrame(customer_orders,"id int,cust_id int,order_date string,sales int")
df.display()

# COMMAND ----------

customer_df = df.groupBy("cust_id").agg(min(col("order_date").cast("date")).alias("mindate"))
customer_df.display()

# COMMAND ----------

df.join(customer_df.alias("c"),"cust_id") \
  .withColumn("newcust",when(col("c.mindate") == col("order_date"),1).otherwise(0)) \
  .withColumn("oldcust",when(col("c.mindate") != col("order_date"),1).otherwise(0)) \
  .groupBy("order_date") \
  .agg(sum(col("newcust")).alias("newcust"),sum(col("oldcust")).alias("oldcust")) \
  .orderBy("order_date") \
  .display()

# COMMAND ----------

data = [
('1', 'in', '2019-12-22 09:00:00'),
('1', 'out', '2019-12-22 09:15:00'),
('2', 'in', '2019-12-22 09:00:00'),
('2', 'out', '2019-12-22 09:15:00'),
('2', 'in', '2019-12-22 09:30:00'),
('3', 'out', '2019-12-22 09:00:00'),
('3', 'in', '2019-12-22 09:15:00'),
('3', 'out', '2019-12-22 09:30:00'),
('3', 'in', '2019-12-22 09:45:00'),
('4', 'in', '2019-12-22 09:45:00'),
('5', 'out', '2019-12-22 09:40:00'),
]

# COMMAND ----------

df = spark.createDataFrame(data,"id string,action string,time string")
df.display()

# COMMAND ----------

df2 = df.select(col("id").cast("int"),"action",to_timestamp(col("time"),"yyyy-MM-dd HH:mm:SS").alias("time"))
df2.display()

# COMMAND ----------

df2.groupBy("id").agg(max("time").alias("maxtime"),max(when(col("action") == "in",col("time"))).alias("intime")) \
   .filter(col("maxtime") == col("intime")) \
   .select("id") \
   .orderBy("id") \
   .display()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table emp_table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE emp_table (
# MAGIC emp_name VARCHAR(20),
# MAGIC Date Date,
# MAGIC Position VARCHAR(20)
# MAGIC );
# MAGIC INSERT INTO emp_table VALUES
# MAGIC ('A', '1999-10-10', 'Clerk'),
# MAGIC ('A', '1992-12-10', 'Agent'),
# MAGIC ('A', '2000-02-01', 'Clerk'),
# MAGIC ('B', '2000-01-01', 'Agent'),
# MAGIC ('B', '2000-02-02', 'Assistant_Manager'),
# MAGIC ('B', '2000-05-15', 'Manager'),
# MAGIC ('C', '2000-05-01', 'Assistant_Manager'),
# MAGIC ('C', '2000-05-06', 'Agent'),
# MAGIC ('C', '2000-07-10', 'Assistant_Manager'),
# MAGIC ('C', '2000-09-15', 'Manager'),
# MAGIC ('D', '2000-02-01', 'Agent'),
# MAGIC ('D', '2000-05-10', 'Assistant_Manager'),
# MAGIC ('D', '2000-06-15', 'Head_Manager');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Designation_table
# MAGIC (
# MAGIC Designation VARCHAR(20),
# MAGIC Designation_order INTEGER
# MAGIC );
# MAGIC INSERT INTO Designation_table VALUES (
# MAGIC 'Clerk', 1),
# MAGIC ('Agent', 2),
# MAGIC ('Assistant_Manager', 3),
# MAGIC ('Manager', 4),
# MAGIC ('Head_Manager', 5);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from designation_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.*,b.Designation_order from emp_table a join designation_table b on a.position = b.designation

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC select a.*,
# MAGIC b.designation_order,
# MAGIC b.designation_order - lag(b.designation_order,1,0) over (partition by emp_name order by date) as ord
# MAGIC from
# MAGIC emp_table a join designation_table b on a.position = b.designation
# MAGIC )
# MAGIC select distinct emp_name
# MAGIC from cte
# MAGIC group by emp_name
# MAGIC having min(ord) >=0

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC select a.*,
# MAGIC b.designation_order,
# MAGIC b.designation_order - lag(b.designation_order,1,0) over (partition by emp_name order by date) as ord
# MAGIC from
# MAGIC emp_table a join designation_table b on a.position = b.designation
# MAGIC )
# MAGIC select distinct emp_name,min(ord)
# MAGIC from cte
# MAGIC group by emp_name
# MAGIC --having min(ord) >=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.*,
# MAGIC b.designation_order,
# MAGIC b.designation_order - lag(b.designation_order,1,0) over (partition by emp_name order by date) as ord,
# MAGIC count(*) over(partition by emp_name) as cnt
# MAGIC from
# MAGIC emp_table a join designation_table b on a.position = b.designation

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select a.*,
# MAGIC b.designation_order,
# MAGIC b.designation_order - lag(b.designation_order,1,0) over (partition by emp_name order by date) as ord,
# MAGIC count(*) over(partition by emp_name) as cnt
# MAGIC from
# MAGIC emp_table a join designation_table b on a.position = b.designation)
# MAGIC SELECT emp_name
# MAGIC FROM cte
# MAGIC WHERE ord >= 0
# MAGIC GROUP BY emp_name
# MAGIC HAVING COUNT(1) = MIN(cnt);

# COMMAND ----------

employee = [
    (1, "Joe", 70000, 3),
     (2, "Hennry", 80000, 4),
     (3, "Sam", 60000, None),
     (4, "Max", 90000, None)
]

# COMMAND ----------

df = spark.createDataFrame(employee,"empid int,empname string,salary int,mgrid int")
df.display()

# COMMAND ----------

df.alias("a").join(df.alias("b"),col("a.empid") == col("b.mgrid")) \
             .filter('b.salary > a.salary') \
             .select("b.empname") \
             .display()

# COMMAND ----------

df.alias("a").join(df.alias("b"),col("a.empid") == col("b.mgrid")) \
             .display()

# COMMAND ----------

tab1 = [
    (1,),(1,),(1,),(2,),(None,),(3,),(3,)
]

# COMMAND ----------

df1 = spark.createDataFrame(tab1,"id int")
df1.display()

# COMMAND ----------

tab2 = [
    (1,),(1,),(None,),(4,),(4,)
]

# COMMAND ----------

df2 = spark.createDataFrame(tab2,"id int")
df2.display()

# COMMAND ----------

df1.join(df2,"id","full").display()

# COMMAND ----------

data = [
    {
       "id" : 1,
       "movie" : "War",
       "description" : "great",
       "rating" : 8.9
    },
       {
       "id" : 2,
       "movie" : "Science",
       "description" : "fiction",
       "rating" : 8.5
    },
    {
       "id" : 3,
       "movie" : "irish",
       "description" : "boring",
       "rating" : 6.2
    },
    {
       "id" : 4,
       "movie" : "Ice Song",
       "description" : "fantasy",
       "rating" : 8.6
    },
    {
       "id" : 5,
       "movie" : "House Card",
       "description" : "intersting",
       "rating" : 9.1
    }
    ]

# COMMAND ----------

df = spark.createDataFrame(data)
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.filter((col("id") % 2 == 1) & (col("description") != "boring")) \
  .orderBy(col("rating").desc()) \
  .display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Window functions

# COMMAND ----------

data = [
    ("spark1","HR",2000),
    ("spark2","HR",3000),
    ("spark3","HR",4000),
    ("spark4","HR",5000),
    ("spark5","Finance",1500),
    ("spark6","Finance",2500),
    ("spark7","Finance",3500),
    ("spark8","Finance",4500),
    ("spark9","Sales",3000),
    ("spark10","Sales",4500),
    ("spark11","Sales",6500),
    ("spark12","Sales",7500)
]

# COMMAND ----------

df = spark.createDataFrame(data,"empname string,dept string,salary int")
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

help(Window)

# COMMAND ----------

from pyspark.sql.functions import 

# COMMAND ----------

window = Window.partitionBy(col("dept")).orderBy(col("salary").desc())

# COMMAND ----------

df2 = df.withColumn("rank",dense_rank().over(window))

# COMMAND ----------

df.filter(dense_rank().over(window) == 3).display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.filter(col("rank") == 3) \
   .drop(col("rank")) \
   .display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL query to display the records with three or more rows with consecutive id's and number of people greater than 100

# COMMAND ----------

data = [
    (1,'2017-07-01',10),
    (2,'2017-07-02',109),
    (3,'2017-07-03',150),
    (4,'2017-07-04',99),
    (5,'2017-07-05',145),
    (6,'2017-07-06',1455),
    (7,'2017-07-07',199),
    (8,'2017-07-09',188)
]

# COMMAND ----------

df = spark.createDataFrame(data,"id int,visit_date string,people int")
df.display()
df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

df2 = df.withColumn("visit_date",to_date("visit_date","yyyy-MM-dd"))
df2.printSchema()

# COMMAND ----------

df2.createOrReplaceTempView("stadium")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stadium

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC *,
# MAGIC row_number() over (order by visit_date) as rnk,
# MAGIC id - row_number() over (order by visit_date) as grp
# MAGIC from stadium
# MAGIC where people > 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from stadium
# MAGIC qualify dense_rank() over (order by people desc) = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC qualify dense_rank() over (order by people desc) =2
# MAGIC from stadium

# COMMAND ----------

# MAGIC %sql
# MAGIC select id,visit_date,people
# MAGIC from stadium

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC id,
# MAGIC visit_date,
# MAGIC people,
# MAGIC lag(people) over (order by people) as previous_value
# MAGIC from
# MAGIC stadium

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC id,
# MAGIC visit_date,
# MAGIC people,
# MAGIC lead(people) over (order by visit_date) as following_value
# MAGIC from stadium

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stadium

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC avg(people) over (order by visit_date rows between 2 preceding and current row) as avgpeople
# MAGIC from stadium

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.withColumn("RollingAvg",avg("people").over(Window.orderBy("visit_date").rowsBetween(-2,0))).display()

# COMMAND ----------

w = Window.orderBy(unix_timestamp("visit_date").rangeBetween(-2,0))

# COMMAND ----------

df2.withColumn("RollingAvg",avg("people").over(Window.orderBy(col("visit_date").cast("timestamp").cast("long")).rowsBetween(-2,0))).display()

# COMMAND ----------

df2.withColumn("unix_timestamp",unix_timestamp("visit_date")).display()

# COMMAND ----------

data = [
    {"id" : 1,
     "date" : '2023-09-01',
     "sales" : 500
     },
    {
     "id" : 2,
     "date" : '2023-09-02',
     "sales" : 1000
    },
    {
     "id" : 3,
     "date" : '2023-09-03',
     "sales" : 1500
    },
    {
     "id" : 4,
     "date" : '2023-09-04',
     "sales" : 2000
    },
    {
     "id" : 5,
     "date" : '2023-09-05',
     "sales" : 2500
    }
]

# COMMAND ----------

df = spark.createDataFrame(data)
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df2 = df.withColumn("date",to_date("date","yyyy-MM-dd"))
df2.printSchema()

# COMMAND ----------

df2.createOrReplaceTempView("rollingsales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rollingsales

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC sum(sales) over(order by date) as rollingsales
# MAGIC from rollingsales

# COMMAND ----------

date2 =  [
    {"id" : 1,
     "date" : '2023-09-01',
     "dept" : "HR",
     "sales" : 500
     },
    {
     "id" : 2,
     "date" : '2023-09-02',
     "dept" : "HR",
     "sales" : 1000
    },
    {
     "id" : 3,
     "date" : '2023-09-03',
     "dept" : "Finance",
     "sales" : 1500
    },
    {
     "id" : 4,
     "date" : '2023-09-04',
     "dept" : "Finance",
     "sales" : 2000
    },
    {
     "id" : 5,
     "date" : '2023-09-05',
     "dept" : "HR",
     "sales" : 2500
    }
]

# COMMAND ----------

new_df = spark.createDataFrame(date2)
new_df.display()

# COMMAND ----------

new_df2 = new_df.withColumn("date",to_date("date","yyyy-MM-dd"))
new_df2.display()

# COMMAND ----------

new_df2.createOrReplaceTempView("rollingsales2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC sum(sales) over(partition by dept order by date) as rollingsales
# MAGIC from rollingsales2
# MAGIC order by dept,date

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn("Previd",lag("id",2).over(Window.orderBy("id"))).display()

# COMMAND ----------

df.withColumn("PrevRating",lag("rating",2).over(Window.orderBy(desc("id")))).display()

# COMMAND ----------

data = [
    (3000,'22-may'),
    (5000,'23-may'),  
    (5000,'25-may'),
    (10000,'22-june'),
    (1250,'03-july')
]

# COMMAND ----------

df = spark.createDataFrame(data,"sales int,month string")
df.display()

# COMMAND ----------

df.select(split(col("month"),'-')[0]).display()

# COMMAND ----------

window = Window.orderBy(split(col("month"),'-')[0]).partitionBy(split(col("month"),'-')[1])

# COMMAND ----------

df.withColumn("cumsales",sum("sales").over(window)).display()

# COMMAND ----------

help(split)

# COMMAND ----------

data2 = [
(4529,'Nancy Young',4125),
(4238,'John Simon',4329),
(4329,'Martina Candreva',4125),
(4009,'Klaus Koch',4329),
(4125,'Mafalda Ranieri',None),
(4500,'Jakub Hrabal',4529),
(4118,'Moira Areas',4952),
(4012,'Jon Nilssen',4952),
(4952,'Sandra Rajkovic',4529),
(4444,'Seamus Quinn',4329)
]

# COMMAND ----------

df2 = spark.createDataFrame(data2,"employee_id int,fullname string,manager_id int")
df2.display()

# COMMAND ----------

df2.createOrReplaceTempView("manager")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from manager

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),manager_id from manager
# MAGIC group by manager_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC b.fullname,a.fullname
# MAGIC from manager a
# MAGIC join manager b on a.employee_id = b.manager_id
# MAGIC --group by b.fullname

# COMMAND ----------

data = [
  {
    "user_id": 1,
    "bio": "Contact John at john@email.com for inquiries.",
    "posts": [
      "Received an email from jane@email.com today.",
      "Check out our website: www.example.com."
    ]
  },
  {
    "user_id": 2,
    "bio": "Email: support@example.com",
    "posts": [
      "Join our mailing list for updates: news@example.com",
      "Follow us on Twitter: @example"
    ]
  }
]

# COMMAND ----------

df = spark.createDataFrame(data)
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select("*",explode("posts").alias("updatedposts")) \
  .drop("posts") \
  .display()

# COMMAND ----------

customers = [
    (1,"Rama",33),
    (2,"Krishna",33),
    (3,"Deekshi",24),
    (4,"Murali",35)
]

# COMMAND ----------

cust_df = spark.createDataFrame(customers,"cust_id int,name string,age int")

# COMMAND ----------

orders = [
    (1,1,100),
    (2,1,200),
    (3,3,400)
]

# COMMAND ----------

orders_df = spark.createDataFrame(orders,"order_id int,cust_id int,sales int")

# COMMAND ----------

cust_df.join(orders_df,cust_df.cust_id == orders_df.cust_id,"left").filter("order_id is null").display()

# COMMAND ----------

players = [
    (1,"Nadal"),
    (2,"Federer"),
    (3,"Djokovic")
]

# COMMAND ----------

players_df = spark.createDataFrame(players,"id int,name string")

# COMMAND ----------

tourneys = [
    (2017,2,1,1,2),
    (2018,3,1,3,2),
    (2019,3,1,1,3)
]

# COMMAND ----------

tourneys_df = spark.createDataFrame(tourneys,"year int,wimbeldon int,frenchopen int,usopen int,ausopen int")

# COMMAND ----------

players_df.display()

# COMMAND ----------

tourneys_df.display()

# COMMAND ----------

unpivotExpr = "stack(4, 'wimbeldon', wimbeldon, 'frenchopen2', frenchopen, 'usopen', usopen,'ausopen',ausopen) as (tournment,id)"

# COMMAND ----------

unPivotDF = tourneys_df.select("year", expr(unpivotExpr))
unPivotDF.display()

# COMMAND ----------

players_df.alias("p").join(unPivotDF.alias("u"),col("p.id") == col("u.id")) \
                     .groupBy(col("p.id"),col("p.name")) \
                     .agg(count("u.id").alias("grandslamswon")) \
                     .orderBy(desc("grandslamswon")) \
                     .display()

# COMMAND ----------

tourneys_df.select("year","wimbeldon").unionAll(tourneys_df.select("year","frenchopen")) \
           .unionAll(tourneys_df.select("year","usopen")) \
           .unionAll(tourneys_df.select("year","ausopen")) \
           .groupBy("wimbeldon") \
           .count() \
           .join(players_df,col("wimbeldon") == col("id")) \
           .select(col("id"),col("name"),col("count").alias("grandslamswon")) \
           .display()

# COMMAND ----------


