# Databricks notebook source
# MAGIC %md
# MAGIC # Data Loading
# MAGIC 
# MAGIC 1. Step 1 - Upload below mentioned 3 files from SampleData folder of this repository to FileStore (This script is using FileStore as source, if you want to store it in some otherlocation then you may need to change the landing_path and dlt_path variables in CMD 2 below)
# MAGIC 
# MAGIC   a) employee.csv
# MAGIC   
# MAGIC   b) department.csv
# MAGIC   
# MAGIC   c) emp_dept.csv
# MAGIC   
# MAGIC 2. Step 2 - Pass the folder name in widget in which files were stored. This will then be used to defined full path for Landing and DLT in CMD 3
# MAGIC 3. Step 3 - Run CMD 5, 7, 9 to see if we are able to successfully read the files
# MAGIC 4. Later follow readme file for further instructions

# COMMAND ----------

#Declare Widget to fetch foldername which will be created in FileStore to save files for this lab
dbutils.widgets.text("Folder_Name","")
folder = dbutils.widgets.get("Folder_Name")

# COMMAND ----------

# Location of Datasets

landing_path = '/FileStore/{0}/landing'.format(folder)
dlt_path = '/FileStore/{0}/dlt'.format(folder)


# COMMAND ----------

# MAGIC %md
# MAGIC # In intial load, we can see we have 3 records for Employee . For EMP_ID we have AGE as -1, we will perform a check on this in DELTA LIVE TABLE NOTEBOOK 02_SQLDLT.sql

# COMMAND ----------

# Read Data From employee.csv files - Initial Run
spark.read.option("header","true").csv(landing_path+"/employee.csv").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # We have 3 records for Department

# COMMAND ----------

# Read Data From department.csv files - Initial Run
spark.read.option("header","true").csv(landing_path+"/department.csv").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # We have 3 records for EMP_DEPT 

# COMMAND ----------

# Read Data From emp_dept.csv files - Initial Run
spark.read.option("header","true").csv(landing_path+"/emp_dept.csv").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Post uploading employee_2.csv file, we can see we have total 6 records

# COMMAND ----------

# Read Data From employee_2 file - Second Run
spark.read.option("header","true").csv(landing_path+"/employee*.csv").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Post uploading employee_3_newschema.csv file, we can see we have total 10 records but with a new schema

# COMMAND ----------

# Read Data From employee_2 file - Second Run
spark.read.option("header","true").csv(landing_path+"/employee*.csv").show()

# COMMAND ----------

#dbutils.fs.rm('/mnt/{0}/landing/employee',True).format(folder)

# COMMAND ----------

#dbutils.fs.rm('/FileStore/gurpreet.sethi@databricks.com/dlt',True)

# COMMAND ----------

#%sql
#DROP DATABASE gsethi_emp_dept CASCADE

# COMMAND ----------

#%sql
#DESCRIBE HISTORY  gsethi_emp_dept.employee_master ;
