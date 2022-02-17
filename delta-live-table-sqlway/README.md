# DELTA LIVE TABLE - SQL Example

This project is to demonstrate Databricks' Delta Live Table functionality using Structured Query Lanugage and CSV sample dataset. This example demonstrates how Delta Live Table can be used to build Medalian architecture (Bronze, Silver and Gold), how to perform Data Quality Checks as part of the Delta Live Table pipeline and how to query results using Databricks SQL Query. Also this example will demonstrate how to pass parameters to the DELTA LIVE TABLE Job and use Delta's Time Travel Feature.

## Initial Data Loading

**Step 1** **:** Upload below mentioned 3 files from SampleData folder of this repository to FileStore 
(This script is using FileStore as source, if you want to store it in some otherlocation then you may need to change the **landing_path** and **dlt_path** variables in **01_Load_Data.py** file).

a) employee.csv

b) department.csv

c) emp_dept.csv

<img width="906" alt="image" src="https://user-images.githubusercontent.com/95003669/154389272-2cab43ef-0b11-49c4-a6db-65276fd5c284.png">

![image](https://user-images.githubusercontent.com/95003669/153998006-f0db0223-7ccb-4a63-b1e3-6b375a765f1c.png)


**Step 2** **:** Pass the folder name in widget in which files were stored. This will then be used to defined full path for Landing and DLT in CMD 3.

![image](https://user-images.githubusercontent.com/95003669/153994838-5ff13532-2b77-4420-8f6e-8ba8f329e92c.png)


**Step 3** **:** Run **CMD 5** , **CMD 7** , **CMD 9** to see if we are able to successfully read the files.

![image](https://user-images.githubusercontent.com/95003669/154378718-44ffbd90-84a2-4096-a039-d69795921989.png)


## Create Delta Live Job

**Step 4** **:** Open "**02_SQLDLT**", in **CMD 1, 2 & 3** we can we have parameterise **foldername**, which means at run time we need to provide this value to construct full path to fecth data from.

![image](https://user-images.githubusercontent.com/95003669/154006177-be387cd3-4026-4491-be08-8736ce6e9c91.png)

**Step 5** **:** Open **Jobs -> Select "Delta Live Tables (Preview) -> Create Pipeline** to create a new Delta Live Table Job using "**02_SQLDLT.sql**" notebook.

![image](https://user-images.githubusercontent.com/95003669/154007620-0d272952-307f-4b8d-a10c-479a111fbb90.png)

**Step 6** **:** Once Create Pipeline window opens, provide below information:

1) Pipeline Name - Name with which we would like to recognize/save this pipeline
2) Notebook Libararies - Location of the **02_SQLDLT.sql** notebook
3) Configuration - 
    pipeline.applyChangesPreviewEnabled = true (As this is Preview Feature at present)
    foldername = Location of the Data (this will then gets replaced in the notebook (as describes in Step 4 above)
4) Target - Database Name (DLT tables - Bronze, Silver & Gold will be logically groupued in this database and hen be accessed in Databricks SQL for query purposes)
5) Storgae Location - Location where we would like to save DELTA LIVE TABLE data (This will be in DELTA Format along with System Folders for Autoload, Schema etc)

![image](https://user-images.githubusercontent.com/95003669/154009166-90348688-d3ff-4371-87ad-d5681913b12a.png)

6) Click **Create** and we can see a screen like below. This is because we just created a pipeline and have not executed yet. Click on **Start** and let the job finish. This may take 3-5 minutes as during this time Databrick SQL will allocate resources for running job and also create database, tables and then execute delta live table notebook. 

![image](https://user-images.githubusercontent.com/95003669/154189583-50096727-b1ee-4484-9dc7-93da85139fd5.png)

7) Once, finished successfully, we can see a Data Lineage Graph and other information like number of records processed, time-took at each step etc.. else troubleshoot errors (should not be the case :).

![image](https://user-images.githubusercontent.com/95003669/154379948-68618b36-3818-431e-966d-2b30c3dc94df.png)

## Explore Data using Databricks SQL

**Step 7** **:** Open "**SQL**" panel from left hand side menu, click **SQL Editor**, in Schema Browser -> hive)metastore -> <Database Name> (Target Name which we have in Step 6 - Point 4 above) and we shall see list of tables (Bronze, Silver and Gold).

![image](https://user-images.githubusercontent.com/95003669/154006681-ddfbdf6c-4bfe-4b4d-9311-5eae94e77196.png)

 ![image](https://user-images.githubusercontent.com/95003669/154194725-f1dd9ec5-6fb2-485b-8cf7-eb5493b8032b.png)
   
    a) department_clean
    b) department_raw
    c) emp_dept_clean
    d) emp_dept_raw
    f) employee_clean
    g) employee_master
    h) employee_raw

### Bronze Layer
**Initial Run** **:** Post initial run, we can see we have 3 records in each of the raw tables (employee_raw, department_raw, emp_dept_raw)

<img width="724" alt="image" src="https://user-images.githubusercontent.com/95003669/154379366-0165f794-ec7d-422b-823b-89cd236da1bc.png">
<img width="724" alt="image" src="https://user-images.githubusercontent.com/95003669/154379417-6533eff3-555d-4e0e-bbef-ac2d5a14fda2.png">
<img width="724" alt="image" src="https://user-images.githubusercontent.com/95003669/154379488-37b2ca83-8e2d-4b96-82bf-416cb7ccf7e9.png">

### Silver Layer
**Initial Run** **:** Post inital run, we can see we have 3 records each for Department and Emp_Dept (department_clean, emp_dept_clean)

![image](https://user-images.githubusercontent.com/95003669/154379549-729a3965-49db-49e3-846a-dc1c2d68242d.png)
![image](https://user-images.githubusercontent.com/95003669/154379579-a47bf15a-48de-4e2f-9b97-1f406ea96ec8.png)
    
**However**, we only have 2 records for Employee (employee_clean)

![image](https://user-images.githubusercontent.com/95003669/154379607-a37bd647-a66f-4165-9764-11e6506fe252.png)

**WHY IS IT SO** Well this because of the **DATA VALIDATION** that we specified in our Delta Lake Notebook which says only load records where AGE >0

![image](https://user-images.githubusercontent.com/95003669/154199375-23ada561-85f8-42ac-981b-a00ad8a1dc24.png)

### Gold Layer
**Initial Run** **:** Post initial run, we can see we have 2 records in gold table (employee_master). This is because GOLD is getting populated from SILVER and in SILVER table we have dropped row where EMP_ID has AGE < 0 (was -1 to be more precise)
    
![image](https://user-images.githubusercontent.com/95003669/154379668-56f611e2-f0d3-461f-b7ee-ebc43e991f4a.png)

## Incremental Load
**Step 8** **:** Upload "**employee_2.csv**" file from SampleData folder of this repository and run **CMD 11** to verify dataset. We can see we have:
    2 UPDATES for EMP_ID = 1 (Age -1 to 44 and Emp_LastName Null to Horton), 3 (Changed Emp_LastName (smith to Smith) 
    1 INSERT for EMP_ID = 4 

![image](https://user-images.githubusercontent.com/95003669/154381761-b84c22b5-0930-4c5c-bff5-7c7bfb2b42cc.png)

Lets run the Delta Live Table Job and see how data will be processed and what results we will see in our Bronze, Silver and Gold tables?

**Step 9** **:** Lets run the Delta Live Table Job. As we can see in below screenshot, only 3 records (Changed Sets) moved to Silver and Gold layer.

![image](https://user-images.githubusercontent.com/95003669/154382139-ec258bbb-383c-447a-b741-356d88cedf77.png)

**Step 10** **:** Lets now explore our Bronze, Silver and Gold tables.

## Bronze
**employee_raw**, we have around 6 records and they are from both of the files. 
![image](https://user-images.githubusercontent.com/95003669/154382228-a1e74551-2f76-4bc4-a58e-3a432f5fd2ce.png)

**department_raw and emp_dept_raw tables**, we have 3 records each (as no new records have been added in those files)
![image](https://user-images.githubusercontent.com/95003669/154382283-8c9258fa-049a-40a7-a5b0-7cdcda934558.png)

![image](https://user-images.githubusercontent.com/95003669/154382330-02dfc609-5a1a-438f-9820-085ec6084c50.png)


## Silver
**employee_clean table**, we have total 5 records this time, We have:
    Emp_Id = 1 - New record appears as in updated dataset we have changed Age from -1 to 44 so this record passes Data Quality Check
    Emp_Id = 3 - We have 2 records which is before and after record, in our Gold Table then we can either present full history or only latest record set
    
![image](https://user-images.githubusercontent.com/95003669/154382368-d5b4e158-915b-4006-9138-c28d4940fe36.png)

**department_raw and emp_dept_raw tables** - have no changes

![image](https://user-images.githubusercontent.com/95003669/154382441-43623974-f0a4-4e93-b578-5550a66b0132.png)

![image](https://user-images.githubusercontent.com/95003669/154382475-a4cf144b-c981-40a7-839e-e2b7a638b0e5.png)

## Gold
**employee_master**, we have 3 records now, we an see Emp_Id = 1 as well as Emp_Id = 3 with recent Employee_Last_Name change 

![image](https://user-images.githubusercontent.com/95003669/154382517-ebce73e2-2472-4a36-9ca2-084518c9427b.png)


## DELTA TIME TRAVEL
Now, lets see how we can leverage DELTA TIME TRAVEL feature to see how changes have happened over the period of time. To do so we can use below synatx:
    
    DESCRIBE HISTORY <Databasename.TableName>

   ![image](https://user-images.githubusercontent.com/95003669/154383265-99364523-8b69-4da1-977a-b2f97ec47bcb.png)


In above diagram we can see we have 3 versions (0, 1, 2) where:
    Version 0 - DLT setup, where databricks enabled DLT feature on the table, a query on version 0 will result no record (check screenshots below)
    Version 1 - We have 2 records written (column - operationMetrics, numOutputRows = 2)
    Version 2 - We have 3 records written (column - operationMetrics, numOutputRows = 3)
    
We can either use **VERSION AS OF <version>** or **TIMESTAMP AS OF <timestamp>** clause while performing **SELECT** on the table.

**Version 0**
    
![image](https://user-images.githubusercontent.com/95003669/154383493-57321be9-2c7f-4d74-ac81-59820fadbd23.png)
    
**Version 1**
![image](https://user-images.githubusercontent.com/95003669/154383542-f5cf7c96-0f53-4dda-bf00-eb78bbda4043.png)
    
**Version 2**
![image](https://user-images.githubusercontent.com/95003669/154383572-801f64a3-b35b-4cb6-bef3-e40d545876c7.png)

    
**Timestamp as of 2022-02-17T00:12:06.000+0000**
    
![image](https://user-images.githubusercontent.com/95003669/154383740-3514cd43-09a0-4db7-b27a-3bd5912bc43c.png)
    
**Timestamp as of 2022-02-17T00:12:35.000+0000**
    
![image](https://user-images.githubusercontent.com/95003669/154383799-e3dc3e96-132c-4236-a8f8-ce7b7c98ef27.png)
    
**Timestamp as of 2022-02-17T00:40:53.000+0000**
    
![image](https://user-images.githubusercontent.com/95003669/154383866-b4192c13-2fc4-46b0-a14b-fb9980d481ee.png)
    
**Note: We can use DATE FUNCTIONS like DATS_SUB, DATE_ADD etc to see data from other dates as well depending on the available history.**

**Same queries can be executed in Databricks SQL**
    
**Describe History**
    
![image](https://user-images.githubusercontent.com/95003669/154384025-b7603569-59cc-45d6-a5b7-68ce7f0013f5.png)


**Version 1**
    
![image](https://user-images.githubusercontent.com/95003669/154384103-e27adbf4-889b-4d76-923d-33adf102573b.png)

**Timestamp as of 2022-02-16 03:21:25**
![image](https://user-images.githubusercontent.com/95003669/154384180-84cd3227-63a8-402a-802b-92c575d11b9b.png)

## More on Delta Live Table
https://docs.microsoft.com/en-us/azure/databricks/data-engineering/delta-live-tables/
    
https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html
    
