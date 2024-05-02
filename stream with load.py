# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, split, col, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
import re
import random
import string

spark = SparkSession.builder.appName("StreamingWithETL").getOrCreate()

# COMMAND ----------

def is_valid(email):
    pattern = r'^[A-Za-z0-9.%+-]+@(yahoo|hotmail|gmail)\.(com|org|net)$'
    return bool(re.match(pattern, email))

# COMMAND ----------

def generate_username(name):
    n=name.lower()
    r=random.randint(100, 999)
    return f"{n}{r}"
def generate_password():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=8))    

# COMMAND ----------

is_valid_udf = udf(is_valid, BooleanType())
generate_username_udf = udf(generate_username, StringType())
generate_password_udf = udf(generate_password, StringType())

# COMMAND ----------

schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Email_id", StringType(), True),
    StructField("Age", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Number", StringType(), True)
])

# COMMAND ----------

lines = spark.readStream.format("text").load("/FileStore/tables/")  

data = lines.select(split(col("value"), " ").alias("data"))

df = data.select(
    col("data").getItem(3).alias("Name"),
    col("data").getItem(8).alias("Email_id"),
    col("data").getItem(10).alias("Age"),
    col("data").getItem(11).alias("Gender"),
    col("data").getItem(12).alias("Number")
)

# COMMAND ----------

dd=df.distinct()

# COMMAND ----------

df_validated = dd.withColumn("Valid_Email", is_valid_udf(col("Email_id"))).filter(col("Valid_Email") == True)

df_fin=df_validated.drop("Valid_Email")

# COMMAND ----------

df_with_username = df_fin.withColumn("Username", generate_username_udf(col("Name")))
df_with_password = df_with_username.withColumn("Password", generate_password_udf())

# COMMAND ----------

query = df_with_password.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write \
        .format("jdbc") \
        .option("url","jdbc:mysql://35.200.153.106/temp") 
        .option("driver", "com.mysql.jdbc.Driver") 
        .option("dbtable", "sample")  
        .option("user", "root")  
        .option("password", "sanjeev@123")  
        .mode("append") 
        .save()).start()

df_with_password.display()
