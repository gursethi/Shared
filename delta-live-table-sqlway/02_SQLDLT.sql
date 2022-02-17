-- Databricks notebook source
CREATE INCREMENTAL LIVE TABLE employee_raw
COMMENT "The raw employee dataset, ingested from /mnt/xxxxx.xxxxxx@databricks.com/landing/employees."
TBLPROPERTIES ("quality" = "bronze")
AS 
SELECT 
  *, 
  reverse(split(input_file_name(), '/'))[0] as Source_Data_File, 
  date_format(from_utc_timestamp(now(), 'Australia/Sydney'), 'yyyy-MM-dd hh:mm:ss') as Record_Insertion_Date 
FROM cloud_files("/FileStore/${foldername}/employee*.csv","csv")

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE department_raw
COMMENT "The raw department dataset, ingested from /mnt/xxxxx.xxxxxx@databricks.com/landing/department."
TBLPROPERTIES ("quality" = "bronze")
AS 
SELECT 
  *,
  reverse(split(input_file_name(), '/'))[0] as Source_Data_File, 
  date_format(from_utc_timestamp(now(), 'Australia/Sydney'), 'yyyy-MM-dd hh:mm:ss') as Record_Insertion_Date 
FROM cloud_files("/FileStore/${foldername}/department*.csv","csv")

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE emp_dept_raw
COMMENT "The raw employee department dataset, ingested from /mnt/xxxxx.xxxxxx@databricks.com/landing/emp_dept."
TBLPROPERTIES ("quality" = "bronze")
AS 
SELECT 
  *,
  reverse(split(input_file_name(), '/'))[0] as Source_Data_File,
  date_format(from_utc_timestamp(now(), 'Australia/Sydney'), 'yyyy-MM-dd hh:mm:ss') as Record_Insertion_Date 
FROM cloud_files("/FileStore/${foldername}/emp_dept*.csv","csv")

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE employee_clean(
  CONSTRAINT age_canot_be_negative EXPECT (age >= 0) ON VIOLATION DROP ROW
)
COMMENT "employee dataset with cleaned-up datatypes / column names and age expectations."
TBLPROPERTIES ("quality" = "silver")
AS SELECT * FROM  stream(live.employee_raw)

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE department_clean(
  CONSTRAINT department_name_not_null EXPECT (department_name IS NOT null) ON VIOLATION DROP ROW
)
COMMENT "department dataset with cleaned-up datatypes / column names expectations."
TBLPROPERTIES ("quality" = "silver")
AS SELECT * FROM stream(live.department_raw)


-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE emp_dept_clean(
  CONSTRAINT emp_id_not_null EXPECT (emp_id IS NOT null) ON VIOLATION DROP ROW,
  CONSTRAINT dept_id_not_null EXPECT (dept_id IS NOT null) ON VIOLATION DROP ROW,
  CONSTRAINT start_date_not_null EXPECT (start_date IS NOT null) ON VIOLATION DROP ROW
)
COMMENT "employee department mapping dataset with cleaned-up datatypes / column names expectations."
TBLPROPERTIES ("quality" = "silver") 
AS SELECT * FROM stream(live.emp_dept_raw)

-- COMMAND ----------

CREATE LIVE TABLE employee_master
COMMENT "Employee Master Dataset"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
    e.Emp_Id as Employee_ID, 
    ec.Emp_FirstName as Employee_First_Name, 
    ec.Emp_LastName as Employee_Last_Name, 
    ec.Age as Employee_Age, 
    d.Department_Name as Employee_Department, 
    ed.Start_Date as Employee_Start_Date,
    ec.Record_Insertion_Date
FROM live.emp_dept_clean as ed
JOIN
    (SELECT Emp_Id, Max(Record_Insertion_Date) as Record_Insertion_Date FROM live.employee_clean Group BY Emp_Id) as e
ON
  ed.Emp_Id = e.Emp_Id
JOIN
 live.employee_clean ec
ON
 ec.Emp_Id = e.Emp_Id
AND
 ec.Record_Insertion_Date = e.Record_Insertion_Date
JOIN
  live.department_clean as d
ON
  ed.Dept_Id = d.Dept_Id
