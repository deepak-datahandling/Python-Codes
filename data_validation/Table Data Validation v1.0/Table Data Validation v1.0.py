# Databricks notebook source
dbutils.widgets.text("Host","","")
dbutils.widgets.text("User","","")
dbutils.widgets.text("Password","","")
dbutils.widgets.text("DatabaseName","","")
dbutils.widgets.text("TableName","","")
dbutils.widgets.text("ADLS_Path","","")

Host = dbutils.widgets.get("Host")
User = dbutils.widgets.get("User")
Password = dbutils.widgets.get("Password")
DatabaseName = dbutils.widgets.get("DatabaseName")
TableName = dbutils.widgets.get("TableName")
ADLS_Path = dbutils.widgets.get("ADLS_Path")

tble_name=DatabaseName + '.' + TableName

print("Host: "+Host)
print("User: "+User)
print("DatabaseName: "+DatabaseName)
print("TableName: "+TableName)
print("ADLS_Path: "+ADLS_Path)

# If you are going to run on your local machine Please assign Required variables before run the entire code. 

import teradatasql
import pandas as pd
import re

#Teradata Credentials
def host_check(host):
    if(host != '<host name>'):
        raise Exception("Invalid Host Name")
try:
    h=Host
    u=User
    ps=Password
    host_check(h)
    con=teradatasql.connect(host=h,user=u,password=ps)
#Connecting with Teradata
    cursor=con.cursor()
except Exception as h:
    raise ConnectionError(h)
#Getting Column Names of a table
col_details='SELECT ColumnName FROM dbc.columns WHERE DatabaseName =? and TableName = ? ORDER BY Columnid'
cursor.execute(col_details,(DatabaseName,TableName))
col_res=cursor.fetchall()

def col_list():
    lk=[]
    for i in range (0,len(col_res)):
        col=re.sub('[^\W]','',str(col_res[i]))
        lk.append(col)
    return lk

columns=col_list()

#Fetching Columns with Decimal Datatype which is used in next Cell Python Code
col_with_D='SELECT ColumnName FROM dbc.columns WHERE DatabaseName =? and TableName = ? and ColumnType = ? ORDER BY Columnid'
cursor.execute(col_details,(DatabaseName,TableName,'D'))
col_D_res=cursor.fetchall() 

def col_D_list():
    l_col_D=[]
    for i in range (0,len(col_D_res)):
        col_D=re.sub('[^\W]','',str(col_D_res[i]))
        l_col_D.append(col_D)
    return l_col_D

deci_col_list=col_D_list()

#Declaring Stats Name into dictionary for final output
dict_list=['count','Null_Count','min','max']
dict1={'count':[],'Null_Count':[],'min':[],'max':[]}

#Summary Stats from teradata
for j in range(0,len(column)):
    drop_query='DROP STATISTICS ON &'.replace('&',tble_name)
    col_stats_query='COLLECT STATISTICS ON & COLUMN ?'.replace('&',tble_name).replace('?',column[j])
    fetch_query='SHOW STATISTICS VALUES COLUMN ? ON &'.replace('&',tble_name).replace('?',column[j])
    
    cursor.execute(drop_query)
    cursor.execute(col_stats_query)
    cursor.execute(fetch_query)
    stats_res=cursor.fetchall()

#Data Cleansing Process
    cln_rec=re.split(',',str(stats_res))
    t=[20,10,15,16]
    counter=0
    for k in t:
        if(k==20):
            f_data=int(re.sub('[^\d]','',str(cln_rec[20])))-int(re.sub('[^\d]','',str(cln_rec[10])))
            dict1[dict_list[counter]].append(f_data)
        else:
            f_data=re.sub('[^\d]','',str(cln_rec[k]))
            dict1[dict_list[counter]].append(f_data)
        counter=counter+1
#Closing the Teradata Connection
con.close()
#Converting Dict to Pandas Dataframe
tera_out=pd.DataFrame(dict1)
#index Renaming
for l in range(0,len(column)):
    tera_out=tera_out.rename(index={l:column[l]})
#Datatype Conversion for Final Comparison
for col in tera_out.columns:
    tera_out[col]=pd.to_numeric(tera_out[col],errors='coerce')
    
print(tera_out)

# COMMAND ----------

import pandas as pd
import pyspark.pandas as ps
L_col=[]
out=[]
null_v=[]

def checkPath(srcPath):
    try:
        if(dbutils.fs.ls(srcPath)):
            return True
        else:
            return False
    except Exception as e:
        raise FileNotFoundError(e)
    
if(checkPath(ADLS_Path)):
    full_df=ps.read_parquet(ADLS_Path + '/*.parquet').to_pandas()
#Decimal Fields Handling
    for col_D in deci_col_list:
        full_df[col_D]=pd.to_numeric(full_df[col_D],errors='coerce')
#Fetching Column Name
    for col in full_df.columns:
        L_col.append(col)
#Getting Stats Details
    for i in range(0,len(L_col)):
        col_name=L_col[i]
        col_list=full_df[col_name]
        null_values=full_df[col_name].isna().sum()
        null_v.append(null_values)
        desc=col_list.describe()
        out.append(desc)
    result=pd.DataFrame(out)
    output['Null_Count']=null_v
    r_stats=['count','Null_Count','min','max']
    temp=[]
    for j in r_stats:
        temp.append(result[j])
    parq_out=pd.DataFrame(temp).T
    print(parq_out)

# COMMAND ----------

#Statistics Comparison

comp=tera_out.compare(parq_out,align_axis=0).rename(index:{'self':'Teradata_Table','other':'Parquet_File'},level=1)

if(comp.empty):
    print("Every Records are Validated Successfully with No Mismatch in Data")
else:
    print(comp)
