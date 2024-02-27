-- Databricks notebook source
drop table if exists namaste_orders;
create table namaste_orders
(
order_id int,
city varchar(10),
sales int
);

create table namaste_returns
(
order_id int,
return_reason varchar(20)
);

insert into namaste_orders
values(1, 'Mysore' , 100),(2, 'Mysore' , 200),(3, 'Bangalore' , 250),(4, 'Bangalore' , 150)
,(5, 'Mumbai' , 300),(6, 'Mumbai' , 500),(7, 'Mumbai' , 800)
;
insert into namaste_returns values
(3,'wrong item'),(6,'bad quality'),(7,'wrong item');

-- COMMAND ----------

with rama as (select r.order_id,r.return_reason,o.city from namaste_orders as o inner join namaste_returns as r on o.order_id= r.order_id
)
select distinct o.city
from namaste_orders as o left join rama dee on o.city = dee.city
where dee.city is null

-- COMMAND ----------

select * from namaste_returns;

-- COMMAND ----------

drop table if exists visits;
Create table If Not Exists
Visits (
user_id int,
visit_date date
);

drop table if exists transactions;
Create table If Not Exists
Transactions (user_id int,
transaction_date date, amount int
);

insert into Visits (user_id, visit_date) values
('1', '2020-01-01'),('2', '2020-01-02'),('12', '2020-01-01'),('19', '2020-01-03')
,('1', '2020-01-02'),('2', '2020-01-03'),('1', '2020-01-04'),('7', '2020-01-11')
,('9', '2020-01-25'),('8', '2020-01-28');

insert into Transactions (user_id, transaction_date, amount) values ('1', '2020-01-02', '120'), ('2', '2020-01-03', '22'),('7', '2020-01-11', '232'),('1', '2020-01-04', '7'),('9', '2020-01-25', '33'),('9', '2020-01-25', '66'),('8', '2020-01-28', '1'),('9', '2020-01-25', '99');


-- COMMAND ----------

select * from visits
order by user_id

-- COMMAND ----------

select * from transactions
order by user_id;

-- COMMAND ----------

--solution 1
select 0 as Transaction_count,
sum(case when t.transaction_date is null then 1 end) as visitscount
from visits v 
left join transactions t on v.user_id = t.user_id and v.visit_date = t.transaction_date
group by 1
union
select 1 as transaction_count,sum(visu) as visitscount
from(
select 
count(*) as visu
from visits v 
inner join transactions t on v.user_id = t.user_id and v.visit_date = t.transaction_date
group by v.user_id,v.visit_date
having count(*) = 1)
union
select 2 as transaction_count,sum(visu) as visitscount
from(
select 
count(*) as visu
from visits v 
inner join transactions t on v.user_id = t.user_id and v.visit_date = t.transaction_date
group by v.user_id,v.visit_date
having count(*) = 2)
union
select 3 as transaction_count,sum(visu) as visitscount
from(
select 
count(*) as visu
from visits v 
inner join transactions t on v.user_id = t.user_id and v.visit_date = t.transaction_date
group by v.user_id,v.visit_date
having count(*) = 3)

-- COMMAND ----------

with cte as (
select 
t.user_id,t.transaction_date,count(1) as cnt
from visits v 
left join transactions t on v.user_id = t.user_id and v.visit_date = t.transaction_date
group by t.user_id,t.transaction_date
)
select case when user_id is null and transaction_date is null then 0 else cnt end as transaction_count,count(*) as visitscount
 from cte
 group by case when user_id is null and transaction_date is null then 0 else cnt end

-- COMMAND ----------

with cte as (select 
v.user_id,v.visit_date,t.user_id as tuser_id,t.transaction_date,
count(*) as cnt
from visits v 
left join transactions t on v.user_id = t.user_id and v.visit_date = t.transaction_date
group by v.user_id,v.visit_date,t.user_id,t.transaction_date
)
select case when tuser_id is null and transaction_date is null then 0 else cnt end as transaction_count,count(*) as visitscount
 from cte
 group by case when tuser_id is null and transaction_date is null then 0 else cnt end

-- COMMAND ----------

insert into tab1 values (1),(1),(2),(2),(1),(1);
insert into tab2 values (1),(1),(2),(4);

-- COMMAND ----------

select * from tab1;

-- COMMAND ----------


select * from tab2;

-- COMMAND ----------

select * from tab1 right join tab2 on tab1.id = tab2.id

-- COMMAND ----------

create table exams (student_id int, subject varchar(20), marks int);

insert into exams values (1,'Chemistry',91),(1,'Physics',91),(1,'Maths',92)
,(2,'Chemistry',80),(2,'Physics',90)
,(3,'Chemistry',80),(3,'Maths',80)
,(4,'Chemistry',71),(4,'Physics',54)
,(5,'Chemistry',79);

-- COMMAND ----------

insert into exams values (6,'Chemistry',75),(6,'Physics',75)

-- COMMAND ----------

with cte as (select 
student_id,subject,marks,
case when marks = lead(marks) over(partition by student_id order by subject) then 1 end as leadsubjects
from exams
where subject in ("Physics","Chemistry")
)
select distinct student_id from cte
where leadsubjects = 1

-- COMMAND ----------

SELECT ex1.student_id FROM exams AS ex1
JOIN exams ex2 ON (ex1.student_id=ex2.student_id AND ex1.marks=ex2.marks)
WHERE ex1.subject IN ('Physics')
AND ex2.subject IN ('Chemistry')

-- COMMAND ----------

drop table if exists people;
create table people(id int,
name varchar(20),
gender char(2));

drop table relations;
create table relations(c_id int,
p_id int
);

insert into people (id, name, gender) values
(107,'Days','F'),
(145,'Hawbaker','M'),
(155,'Hansel','F'),
(202,'Blackston','M'),
(227,'Criss','F'),
(278,'Keffer','M'),
(305,'Canty','M'),
(329,'Mozingo','M'),
(425,'Nolf','M'),
(534,'Waugh','M'),
(586,'Tong','M'),
(618,'Dimartino','M'),
(747,'Beane','M'),
(878,'Chatmon','F'),
(904,'Hansard','F');

insert into relations(c_id, p_id) values
(145, 202),
(145, 107),
(278,305),
(278,155),
(329, 425),
(329,227),
(534,586),
(534,878),
(618,747),
(618,904);

-- COMMAND ----------

with cte as(select 
p.name,
r.p_id
from 
people p join relations r on r.c_id = p.id
)
select 
c.name as child,p.name as father,
p.gender
from cte c join people p on c.p_id = p.id

-- COMMAND ----------

select 
p.name,
r.p_id
from 
people p join relations r on r.c_id = p.id

-- COMMAND ----------

SELECT p1.name AS child
 ,MAX(CASE WHEN p2.gender = 'M' THEN p2.name END) AS father
 ,MAX(CASE WHEN p2.gender = 'F' THEN p2.name END) AS mother
FROM people p1
INNER JOIN relations r ON p1.id = r.c_id
INNER JOIN people p2 ON r.p_id = p2.id
GROUP BY p1.name
ORDER BY child;

-- COMMAND ----------

drop table travel;

-- COMMAND ----------

CREATE TABLE travel ( source VARCHAR(512),
  destination VARCHAR(512),
  distance INT
);
INSERT INTO travel VALUES ('Mumbai', 'Bangalore', '500'),
('Bangalore', 'Mumbai', '500'),
('Delhi', 'Mathura', '150'),
('Mathura', 'Delhi', '150'),
('Nagpur', 'Pune', '500'),
('Pune', 'Nagpur', '500'),
("Mumbai","Hyderabad",'750')

-- COMMAND ----------

WITH cte1
AS (
SELECT *
,ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn
FROM travel
)
SELECT c1.source
,c1.destination
,c1.distance
FROM cte1 c1
INNER JOIN cte1 c2 ON c2.destination = c1.source
AND c1.rn < c2.rn;

-- COMMAND ----------

create table airlines_data(airline_id int, airline_name string);

insert into airlines_data values (1, "Airline A"),(2, "Airline B"),(3, "Airline C");

-- COMMAND ----------

create table flights_data(flight_id int,airline_id int,destination_airport_id int);
insert into flights_data values  (1, 1, 101),  (2, 1, 102),  (3, 2, 101), (4, 2, 103),(5, 3, 101),  (6, 3, 102), (7, 3, 103);

-- COMMAND ----------

select * from flights_data

-- COMMAND ----------

select airline_id
from flights_data
group by airline_id
having count(distinct destination_airport_id) = (select count(distinct destination_airport_id) from flights_data)

-- COMMAND ----------

create table medications(medication_id int , medication_name String);

insert into medications values (1, "Medication A"),(2, "Medication B"),(3, "Medication C"),(4, "Medication D"),(5, "Medication E");

-- COMMAND ----------

select * from medications

-- COMMAND ----------

create table prescriptions (prescription_id int, doctor_id int, medication_id int);

insert into prescriptions values (1, 1, 1),(2, 2, 1),(3, 3, 1), (4, 1, 2), (5, 2, 2), (6, 3, 2),
    (7, 1, 3),(8, 2, 4),(9, 3, 4),(10, 4, 5),(11, 5, 5),(12, 6, 5);

-- COMMAND ----------

select * from prescriptions;

-- COMMAND ----------

select medication_name
from prescriptions p join medications m on m.medication_id = p.medication_id
group by medication_name
having count(distinct doctor_id) >= 3

-- COMMAND ----------

create table sku
(
sku_id int,
price_date date ,
price int
);

insert into sku values
(1,'2023-01-01',10)
,(1,'2023-02-15',15)
,(1,'2023-03-03',18)
,(1,'2023-03-27',15)
,(1,'2023-04-06',20);

-- COMMAND ----------

select * from sku

-- COMMAND ----------

select
sku_id,
price_date,
date_trunc('MONTH',price_date) as truncdate,
lag(price) over(order by price_date) as prevprice,
case when price_date = date_trunc('MONTH',price_date) then price else lag(price) over(order by price_date) end as newprice,
price
from
sku

-- COMMAND ----------

create table teams(empid int,teamid int)

-- COMMAND ----------

insert into teams values (1,8),(2,8),(3,8),(4,7),(5,9),(6,9)

-- COMMAND ----------

with cte as (
  select teamid,count(*) as cnt
  from teams
  group by teamid
)
select empid,cnt
from teams t join cte c on c.teamid = t.teamid
order by empid

-- COMMAND ----------

create table flights(id int,flight_id string,origin string,destination string);

-- COMMAND ----------

insert into flights values (1,"flight1",'Delhi',"Hyderabad"),(1,"flight2","Hyderabad","Kochi"),(1,"flight3","Kochi","Bangalore"),
(2,"flight1","Mumbai","Ayodhya"),(2,"flight2","Ayodhya","Kolkata")

-- COMMAND ----------

select * from flights

-- COMMAND ----------

with cte as (
  select id,
  min(flight_id) as origin,
  max(flight_id) as destination
  from flights
  group by id
)
select 
f.id,
max(case when f.flight_id = c.origin then f.origin end) as origin,
max(case when f.flight_id = c.destination then f.destination end) as destination
from 
flights f join cte c on f.id = c.id
group by f.id

-- COMMAND ----------

select id,first(origin),last(destination) from flights
group by id

-- COMMAND ----------

create table amazon_interview (
user_id int,
item varchar(50),
created_at date,
revenue bigint
);



-- COMMAND ----------

insert into amazon_interview(user_id , item , created_at, revenue)
values(100, 'bread', '2020-03-07', 410),(100, 'banana' ,'2020-03-13' ,175),
(100, 'banana',"2023-03-29", 599),
(101, 'milk', '2020-03-01', 449),
(101, 'milk', '2020-03-26', 740),
(114, 'banana', '2020-03-10', 200),
(114, 'biscuit', '2020-03-16', 300);

-- COMMAND ----------

with cte as 
(select 
a.*,
lead(created_at) over(partition by user_id order by created_at) as nextday,
date_diff(day,created_at , lead(created_at) over(partition by user_id order by created_at)) as diff
from amazon_interview a
)
select distinct user_id
from cte
where diff <= 7

-- COMMAND ----------

CREATE TABLE TRAVEL_DATA(customer string,start_loc string,end_loc string);

-- COMMAND ----------

insert into TRAVEL_DATA values ("c1","New York","Lima"),("c1","London","New York"),("c1","Lima","Sao Paulo"),
("c1","Sao Paulo","New Delhi"),("c2","Mumbai","Hyderabad"),("c2","Surat","Pune"),("c2","Hyderabad","Surat"),
("c3","Kochi","Kurnool"),("c3","Lucknow","Agra"),("c3","Agra","Jaipur"),("c3","Jaipur","Kochi");

-- COMMAND ----------

select * from TRAVEL_DATA

-- COMMAND ----------

with cte as (
  select customer,start_loc
  from travel_data
),
cte1 as (
   select customer,end_loc
  from travel_data
)
select 
t1.customer,
max(case when c.end_loc is null then t1.start_loc end) as startlocation,
max(case when c1.start_loc is null then t1.end_loc end) as endlocation,
count(t1.customer)+1 as locations
from travel_data t1 left join cte1 c on t1.customer = c.customer and t1.start_loc = c.end_loc
left join cte c1 on t1.end_loc = c1.start_loc
group by t1.customer

-- COMMAND ----------

CREATE TABLE Purchases(purchase_id int,customer_id int,purchase_date DATE);

-- COMMAND ----------

INSERT INTO Purchases values (1,101,'2024-01-01'),(2,102,'2024-01-02'),(3,101,'2024-01-02'),(4,103,'2024-01-03'),(5,101,'2024-01-03'),(6,104,'2024-01-04'),(7,102,'2024-01-04'),(8,103,'2024-01-05'),(9,102,'2024-01-05'),(10,103,'2024-01-06'),(11,102,'2024-01-06'),(12,107,'2024-01-07')

-- COMMAND ----------

select * from purchases

-- COMMAND ----------

select customer_id,purchase_date from purchases
order by 1 asc,2 asc

-- COMMAND ----------

with cte as (select 
customer_id,
purchase_date,
case when lead(purchase_date,1) over (partition by customer_id order by customer_id,purchase_date) is null then purchase_date
else lead(purchase_date,1) over (partition by customer_id order by customer_id,purchase_date) end as leaddate,
case when lead(purchase_date,1) over (partition by customer_id order by customer_id,purchase_date) is null then purchase_date
else lead(purchase_date,2) over (partition by customer_id order by customer_id,purchase_date) end as leaddate2
from purchases
)
select distinct customer_id
from cte
where date_diff(day,purchase_date,leaddate ) = 1 and date_diff(day,leaddate,leaddate2) = 1 

-- COMMAND ----------

with cte as (select 
customer_id,
purchase_date,
row_number() over (partition by customer_id order by purchase_date) as rno,
day(purchase_date) as days,
day(purchase_date) - row_number() over (partition by customer_id order by purchase_date) as daysdiff
from purchases
)
select customer_id
from cte
group by customer_id,daysdiff
having count(*) > 2

-- COMMAND ----------



-- COMMAND ----------

drop table t1;
create table t1(id int);

-- COMMAND ----------

insert into t1 values (1),(1),(1),(0),(0),(null),(null)

-- COMMAND ----------

select * from t1

-- COMMAND ----------

create table t2(id int);
insert into t2 values(1),(1),(0),(null)

-- COMMAND ----------

select *
from
t1 join t2 on t1.id = t2.id

-- COMMAND ----------

select *
from
t1 left join t2 on t1.id = t2.id

-- COMMAND ----------

select *
from
t1 right join t2 on t1.id = t2.id

-- COMMAND ----------

select *
from
t1 full join t2 on t1.id = t2.id

-- COMMAND ----------

create table tab1(id int);

-- COMMAND ----------

insert into tab1 values (1),(1),(1);

-- COMMAND ----------

create table tab2(id int);
insert into tab2 values (1),(1);

-- COMMAND ----------

select * from tab1 join tab2 on tab1.id = tab2.id

-- COMMAND ----------

select * from tab1 left join tab2 on tab1.id = tab2.id

-- COMMAND ----------

select * from tab1 right join tab2 on tab1.id = tab2.id

-- COMMAND ----------

create table clocked_hours(
empd_id int,
swipe time,
flag char
);
insert into clocked_hours values
(11114,'08:30','I'),
(11114,'10:30','O'),
(11114,'11:30','I'),
(11114,'15:30','O'),
(11115,'09:30','I'),
(11115,'17:30','O');

-- COMMAND ----------

create table customer_orders (
order_id integer,
customer_id integer,
order_date date,
order_amount integer
);

insert into customer_orders values(1,100,cast('2022-01-01' as date),2000),
(2,200,cast('2022-01-01' as date),2500),(3,300,cast('2022-01-01' as date),2100),
(4,100,cast('2022-01-02' as date),2000),(5,400,cast('2022-01-02' as date),2200),
(6,500,cast('2022-01-02' as date),2700),(7,100,cast('2022-01-03' as date),3000),
(8,400,cast('2022-01-03' as date),1000),(9,600,cast('2022-01-03' as date),3000);

-- COMMAND ----------

select * from customer_orders

-- COMMAND ----------

with cte as (
  select customer_id,min(order_date) as odate
  from customer_orders
  group by customer_id
)
select 
c.order_date,count(odate) as newcust,count(order_date) - count(odate) as repcust,
sum(case when c.order_date = odate then c.order_amount end) as newcustamount,
sum(case when odate is null then c.order_amount end) as oldcustamount
from customer_orders c
left join cte on cte.odate = c.order_date and cte.customer_id = c.customer_id
group by c.order_date
order by c.order_date

-- COMMAND ----------

with cte as (
  select customer_id,min(order_date) as odate
  from customer_orders
  group by customer_id
)
select 
*
from customer_orders c
left join cte on cte.odate = c.order_date and cte.customer_id = c.customer_id

-- COMMAND ----------

CREATE TABLE Employee(
emp_id INT,
emp_name VARCHAR(25),
salary INT,
dep_id INT
);

INSERT INTO Employee(emp_id, emp_name, salary, dep_id) VALUES
(1001, 'Marlania', 92643, 1),
(1002, 'Briana', 87202, 1),
(1003, 'Maysha', 70545, 1),
(1004, 'Jamacia', 65285, 1),
(1005, 'Kimberli', 51407, 2),
(1006, 'Lakken', 88933, 2),
(1007, 'Micaila', 82145, 2),
(1008, 'Gion', 66187, 2),
(1009, 'Latoynia', 55729, 3),
(1010, 'Shaquria', 52111, 3),
(1011, 'Tarvares', 82979, 3),
(1012, 'Gabriella', 74132, 4),
(1013, 'Medusa', 72551, 4),
(1014, 'Kubra', 55170, 4);

-- COMMAND ----------

select * from employee

-- COMMAND ----------

select * from employee

-- COMMAND ----------

with cte as(select *,
dense_rank() over (partition by dep_id order by salary desc) as rnk
from employee
)
select  
from employee e join cte c on e.dep_id = c.dep_id
where rnk = 

-- COMMAND ----------

with cte as(select dep_id,
case when salary = max(salary) over (partition by dep_id) then emp_name end as maxemp,
case when salary = min(salary) over (partition by dep_id) then emp_name end as minemp
from employee
)
select dep_id,max(maxemp),max(minemp)
from cte
group by dep_id

-- COMMAND ----------

select dep_id,
max(first_value(emp_name) over (partition by dep_id order by salary desc)) as maxemp
from employee
group by dep_id

-- COMMAND ----------

create table employees
(id int,
name string,
salary int,
managerid int);

-- COMMAND ----------

insert into employees(id,name,salary,managerid) values (1,"Joe",70000,3),(2,"Henry",80000,4),(3,"Sam",60000,Null),(4,"Max",90000,Null)

-- COMMAND ----------

select y.name from employees e join employees y
on e.id = y.managerid
where y.salary > e.salary

-- COMMAND ----------

select * from employees e join employees y
on e.id = y.managerid

-- COMMAND ----------

create table people
(id int,
name varchar(20),
gender varchar(1));

-- COMMAND ----------

create table relations
(
c_id int,
p_id int
);

-- COMMAND ----------

insert into people (id, name, gender)
values
(107,'Days','F'),
(145,'Hawbaker','M'),
(155,'Hansel','F'),
(202,'Blackston','M'),
(227,'Criss','F'),
(278,'Keffer','M'),
(305,'Canty','M'),
(329,'Mozingo','M'),
(425,'Nolf','M'),
(534,'Waugh','M'),
(586,'Tong','M'),
(618,'Dimartino','M'),
(747,'Beane','M'),
(878,'Chatmon','F'),
(904,'Hansard','F'),
(666,'Rama','M'),
(777,'Sisira','F');

-- COMMAND ----------


insert into relations(c_id, p_id)
values
(145, 202),
(145, 107),
(278,305),
(278,155),
(329, 425),
(329,227),
(534,586),
(534,878),
(618,747),
(618,904),
(777,666)

-- COMMAND ----------

select * from people

-- COMMAND ----------

select * from relations

-- COMMAND ----------

with f as (
  select r.c_id,p.name as father from people p left join relations r on p.id = r.p_id
  where p.gender = 'M' and r.c_id is not null
),
m as (
   select r.c_id,p.name as mother from people p left join relations r on p.id = r.p_id
  where p.gender = 'F' and r.c_id is not null
),
child as (select f.c_id,father,mother
from f join m on f.c_id = m.c_id
)
select 
p.name as child,father,mother 
from people p join child c on c.c_id = p.id
order by p.name

-- COMMAND ----------

with f as (
  select r.c_id,p.name as father from people p left join relations r on p.id = r.p_id
  where p.gender = 'M'
),
m as (
   select r.c_id,p.name as mother from people p left join relations r on p.id = r.p_id
  where p.gender = 'F' 
)
select p.name as child,father,mother
from people p left join f on p.id = f.c_id left join m on p.id = m.c_id
where father is not null or mother is not null
order by p.name

-- COMMAND ----------

 select r.c_id,p.name as mother from people p left join relations r on p.id = r.p_id
  where p.gender = 'F'

-- COMMAND ----------

  select * from people p left join relations r on p.id = r.p_id
  where p.gender = 'M' and r.c_id is not null

-- COMMAND ----------

  customer_id int,
  transaction_date date,
  amount decimal(10, 2)
);

insert into Transactions (customer_id, transaction_date, amount)
values
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
  (106, '2023-05-23', 70.00);

-- COMMAND ----------

select * from transactions

-- COMMAND ----------

with cte as (select *,
lag(transaction_date,1,transaction_date) over (partition by customer_id order by transaction_date) as prevdate,
date_diff(day,lag(transaction_date,1,transaction_date) over (partition by customer_id order by transaction_date),transaction_date) as diff
from transactions
order by customer_id
)
select *
from cte

-- COMMAND ----------

with cte as(
select *,(day(transaction_date)-row_number() over(partition by customer_id order by customer_id)) as rn from Transactions),
cte2 as(
select *,row_number() over(partition by customer_id,rn order by customer_id) as rn1,count(8) over(partition by customer_id,rn) as cn from cte)
select customer_id,min(transaction_date) as mn,max(transaction_date) as mx
from cte2
where cn >2
group by customer_id,cn;

-- COMMAND ----------

with cte as(
select *,(day(transaction_date)-row_number() over(partition by customer_id order by customer_id)) as rn from Transactions),
cte2 as(
select *,count(*) over(partition by customer_id,rn) as cn from cte)
select customer_id,min(transaction_date) as mn,max(transaction_date) as mx
from cte2
where cn >2
group by customer_id,cn;

-- COMMAND ----------

select *,
day(transaction_date),
row_number() over(partition by customer_id order by customer_id) as rn
from transactions

-- COMMAND ----------

select *,(day(transaction_date)-row_number() over(partition by customer_id order by customer_id)) as rn from Transactions

-- COMMAND ----------

with cte as(
select *,(day(transaction_date)-row_number() over(partition by customer_id order by customer_id)) as rn from Transactions)
select *,row_number() over(partition by customer_id,rn order by customer_id) as rn1,count(8) over(partition by customer_id,rn) as cn from cte

-- COMMAND ----------

with cte as (select * ,
day(transaction_date) - row_number() over (partition by customer_id order by transaction_date) as rn
from transactions
),
cte2 as (select *,count(*) over (partition by customer_id,rn) as cnt
from cte
)
select customer_id,max(transaction_date),min(transaction_date) from cte2
where cnt > 2
group by customer_id,cnt

-- COMMAND ----------

create table players_table (id int , event varchar(20) , year int , gold varchar(30) , silver varchar(30) , bronze varchar(30)); 

insert into players_table values 
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
(12 , '500m' , 2016 , 'Thomas' , 'Steven' , 'Catherine');

-- COMMAND ----------

select * from players_table

-- COMMAND ----------

select g.gold,count(*)
from players_table g
where g.gold not in (select distinct s.silver from players_table s union select distinct b.bronze from players_table b)
group by g.gold

-- COMMAND ----------

select
distinct g.*
from players_table g left join players_table s on g.gold = s.silver
left join players_table b on g.gold = b.bronze
where s.silver is null and b.bronze is null

-- COMMAND ----------

create table stadium(id   int ,
visit_date date,
people int  
)

-- COMMAND ----------

insert into stadium values(1,'2017-01-01',10),(2,'2017-01-02',109),(3,'2017-01-03',150),(4,'2017-01-04',99),(5,'2017-01-05',145),
(6,'2017-01-06',1455),(7,'2017-01-07',199),(8,'2017-01-09',188)

-- COMMAND ----------

select * from stadium

-- COMMAND ----------

with cte as (select 
id,
people,
row_number() over (order by id) as rowid,
id - row_number() over (order by id) as diff
 from stadium
 where people > 100
),
diff as (select diff
from cte
group by diff
having count(*) >= 3
)
select id,people from cte 
where diff in (select * from diff)

-- COMMAND ----------

create table emp(
emp_id int,
emp_name varchar(20),
department_id int,
salary int,
manager_id int,
emp_age int);

insert into emp values (1, 'Ankit', 100,10000, 4, 39);
insert into emp values (2, 'Mohit', 100, 15000, 5, 48);
insert into emp values (3, 'Vikas', 100, 10000,4,37);
insert into emp values (4, 'Rohit', 100, 5000, 2, 16);
insert into emp values (5, 'Mudit', 200, 12000, 6,55);
insert into emp values (6, 'Agam', 200, 12000,2, 14);
insert into emp values (7, 'Sanjay', 200, 9000, 2,13);
insert into emp values (8, 'Ashish', 200,5000,2,12);
insert into emp values (9, 'Mukesh',300,6000,6,51);
insert into emp values (10, 'Rakesh',300,7000,6,50);

-- COMMAND ----------

select * from emp

-- COMMAND ----------

with departmentavg as (
  select department_id,avg(salary) as deptavg
  from emp
  group by department_id
),
CompanyAverages AS ( 
SELECT D.department_id,
AVG(E.salary) AS company_avg 
FROM emp E JOIN departmentavg D 
ON E.department_id <> D.department_id 
GROUP BY D.department_id ) 
select e.* from emp e join departmentavg d on e.department_id = d.department_id
where d.deptavg < (select avg(salary) from emp )

-- COMMAND ----------

with departmentavg as (
  select department_id,avg(salary) as deptavg
  from emp
  group by department_id
)
SELECT *
FROM emp E JOIN departmentavg D 
ON E.department_id <> D.department_id 
order by E.department_id

-- COMMAND ----------

create table customer_orders (
order_id integer,
customer_id integer,
order_date date,
order_amount integer
);

-- COMMAND ----------

insert into customer_orders values(1,100,cast('2022-01-01' as date),2000),(2,200,cast('2022-01-01' as date),2500),(3,300,cast('2022-01-01' as date),2100)
,(4,100,cast('2022-01-02' as date),2000),(5,400,cast('2022-01-02' as date),2200),(6,500,cast('2022-01-02' as date),2700)
,(7,100,cast('2022-01-03' as date),3000),(8,400,cast('2022-01-03' as date),1000),(9,600,cast('2022-01-03' as date),3000)

-- COMMAND ----------

select * from customer_orders

-- COMMAND ----------

 select customer_id,min(order_date) as mindate
  from customer_orders
  group by customer_id

-- COMMAND ----------

with cte as (
  select customer_id,min(order_date) as mindate
  from customer_orders
  group by customer_id
)
select 
order_date,
sum(case when order_date = cte.mindate then 1 else 0 end) as newcust,
sum(case when cte.mindate <> order_date then 1 else 0 end) as oldcust
from customer_orders c left join cte on c.customer_id = cte.customer_id
group by order_date
order by 1

-- COMMAND ----------

WITH cte AS (
SELECT customer_id, MIN(order_date) AS first_order
FROM customer_orders
GROUP BY customer_id)
SELECT c.order_date
,SUM(CASE
WHEN c.order_date = cte.first_order THEN 1 ELSE 0 END) AS new_customer
,SUM(CASE
WHEN c.order_date != cte.first_order THEN 1 ELSE 0 END) AS repeated_customer
FROM customer_orders c
LEFT JOIN cte
ON c.customer_id = cte.customer_id
GROUP BY c.order_date;

-- COMMAND ----------

create table fruits(dates date,fruits string,itemssold int);

-- COMMAND ----------

insert into fruits values ("2023-09-01","Apples",10);
insert into fruits values ("2023-09-01","Oranges",5);
insert into fruits values ("2023-09-02","Apples",20);
insert into fruits values ("2023-09-02","Oranges",12);
insert into fruits values ("2023-09-03","Apples",5);
insert into fruits values ("2023-09-03","Oranges",20);

-- COMMAND ----------

select
dates 
,sum(case when fruits = "Apples" then itemssold end) - sum(case when fruits = "Oranges" then itemssold end) as diff
from fruits
group by dates
order by dates

-- COMMAND ----------

create table hospital ( emp_id int
, action varchar(10)
, time timestamp);

insert into hospital values ('1', 'in', '2019-12-22 09:00:00');
insert into hospital values ('1', 'out', '2019-12-22 09:15:00');
insert into hospital values ('2', 'in', '2019-12-22 09:00:00');
insert into hospital values ('2', 'out', '2019-12-22 09:15:00');
insert into hospital values ('2', 'in', '2019-12-22 09:30:00');
insert into hospital values ('3', 'out', '2019-12-22 09:00:00');
insert into hospital values ('3', 'in', '2019-12-22 09:15:00');
insert into hospital values ('3', 'out', '2019-12-22 09:30:00');
insert into hospital values ('3', 'in', '2019-12-22 09:45:00');
insert into hospital values ('4', 'in', '2019-12-22 09:45:00');
insert into hospital values ('5', 'out', '2019-12-22 09:40:00');

-- COMMAND ----------

select 
emp_id,
max(time),
max(case when action = "in" then time end) as inmaxtime
from
hospital
group by emp_id
order by 1

-- COMMAND ----------

select 
emp_id
from
hospital
group by emp_id
having max(time) = max(case when action = 'in' then time end)
order by emp_id

-- COMMAND ----------


