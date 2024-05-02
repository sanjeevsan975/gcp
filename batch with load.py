# Databricks notebook source
import pyspark
from pyspark import SparkConf, SparkContext
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit,monotonically_increasing_id,udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,BooleanType
import re
import random
import string

# COMMAND ----------

sc = SparkContext.getOrCreate()
ss=SparkSession.builder.getOrCreate() 
text = sc.textFile('/FileStore/tables/dupli.txt')

# COMMAND ----------

m=text.flatMap(lambda x:x.split(","))
m.collect()

# COMMAND ----------

name=m.map(lambda x: x.split(" ")[3])
email=m.map(lambda x: x.split(" ")[8])
age=m.map(lambda x: x.split(" ")[10])
gen=m.map(lambda x:x.split(" ")[11])
ph=m.map(lambda x:x.split(" ")[12])

# COMMAND ----------

add=list(zip(name.collect(),email.collect(),age.collect(),gen.collect(),ph.collect()))
df=ss.createDataFrame(add,["Name","Email_id","Age","Gender","Number"])

# COMMAND ----------

df.show()

# COMMAND ----------

df.count()

# COMMAND ----------

dff=df.distinct()
dff.show()

# COMMAND ----------

dff.count()

# COMMAND ----------

def isvalid(email):
    p=r'^[A-Za-z0-9._%+-]+@(yahoo|hotmail|email|gmail)\.(com|org|net)$'
    return bool(re.match(p,email))
vmail=udf(isvalid,BooleanType())
vm=dff.withColumn("validmail",vmail(dff["Email_ID"]))
vm.show()

# COMMAND ----------

fs=vm.filter(vm["validmail"]== True)
res=fs.drop("validmail")

# COMMAND ----------

res.show()

# COMMAND ----------

res.count()

# COMMAND ----------

data=res.withColumn("Id",monotonically_increasing_id()+1001)
data=data.select("Id",*dff.columns)
data.show()

# COMMAND ----------

def genuser(name):
    fl=name.lower()
    rand=random.randint(100,999)
    us=f"{fl}{rand}"
    return us


# COMMAND ----------

guser=udf(genuser,StringType())
dfuser=data.withColumn("Username",guser(df["Name"]))

# COMMAND ----------

dfuser.show()

# COMMAND ----------

def genpass():
    ln=8
    passc=string.ascii_letters+string.digits
    p=''.join(random.choice(passc) for i in range(ln))
    return p

# COMMAND ----------

genp=udf(genpass,StringType())
dfp=dfuser.withColumn("Password",genp())

# COMMAND ----------

dfp.show()


# COMMAND ----------

driver = "com.mysql.cj.jdbc.Driver"
url = "jdbc:mysql://35.200.196.223"
table = "demo.demo123"
user = "root"
password = "sanjeev@123"
 
dfp.write.format("jdbc").option("driver", driver).option("url",url).option("dbtable", table).option("mode", "append").option("user",user).option("password", password).save()

# COMMAND ----------

    
34.134.239.196
Pa$$w0rd
34.134.239.196/32
34.122.141.137