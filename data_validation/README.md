# Table Data Validation

## Requirement
  As a Business User I want to Archive my old Teradata tables which is having huge records and not active now, into Azure Data Lake Storage. Also, I wants to do table data validation on the Archived data.
  
## Key Points to Remember
1. I developed this code in Azure Databricks Environment, So you need to give the database credentials on the required fields in starting itself.Also, You can see I used dbutils to automate the process in ADF and if you don't want, you can ignore and manually assign the credentials.
2. Make sure you are having direct table access and giving table names only, Because on top teradata views we couldn't get the Summary Stats.

### Introduction:
  Table Data Validation is the process of verifying the Table data which was migrated from Legacy system to Cloud(Azure) and vice versa. In this Data Validation process we will get Summary Stats of a Base table using normal SQL Queries and we will get the Summary Stats Parquet files which is moved from Legacy to Cloud(Azure).To Achive the entire process we Developed a Python Code and detailed explaination of the code mentioned below. 

### Summary Stats:
  Summary Statistics is nothing but, the detailed information of a table in column level.
For example,
            We can get the Count,Max value,Min value,Number of Nulls,Average etc., of Salary column in a Employee table. 
  
### How The Table Data Validation Works:
1. The Python Code will first connect with the required database like MySQL,Teradata,DB2 etc., using python libraries.
2. Once the connection was established we can perform SQL inside the Databases from here.
3. Then we will collect the Details we wanted(Summary Stats) and store it in a dictionary format(Because easy to convert into Pandas Dataframe) and convert that into Pandas Dataframe
4. Now, we will read the moved table data (Parquet files) and make it as a Pandas Dataframe.
5. Usinge Pandas Dataframe functions we will get the Summary Stats on Parquet files
6. As a final stage we will compare the both dataframe and print the results.

### Pre-Requisites:
* Teradata Access and Credentials --> To connect with database and collect the summary stats
* Azure Data Factory --> To migrate the table data using Copy Data Activity
* Azure Data Lake Storage --> To store the Archiving table data
* Azure Databricks --> To perform Table Data Validation once, Archival is done.

### Concepts used in Table Data Validation Code:
* Cursor method in python.
* Pandas dataframe
* Collections and Loops in python
* Pyspark 

### PROS:
1. At a single time it can be able to compare millons of records and give you the results.
2. We can use more than single stats like count,min,max etc.,
3. Time consumption was less.
4. we can avoid human errors while comparing the records.

### CONS:
1. Perfermance will be less compared to Pyspark.
2. No Optimization techniques used for now.
 
### Features can be added:
1. Here while entereing the Teradata password it was visible, so to avoid this we can go for dbutils secrets.
2. Since we are predominantly using Pandas dataframe the performance may be less compared to Pyspark, So I was trying to implement the same in Pyspark.
3. Mail features can be added and am working on it. 

# NOTE:
  As a fresher, I need your support here and also any New Suggestions or Ideas are always welcome!
  Please feel free to ask any queries or doubts in Chats/Email.


                                                                    # THANK YOU
