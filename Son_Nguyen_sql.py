# Databricks notebook source
dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

dbutils.fs.cp("FileStore/tables/clinicaltrial_2023.zip", "file:/tmp/")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp/

# COMMAND ----------

# MAGIC %sh 
# MAGIC unzip -d /tmp/  /tmp/clinicaltrial_2023.zip

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls /tmp/

# COMMAND ----------

dbutils.fs.mkdirs("FileStore/tables/clinical_trials/")

# COMMAND ----------

dbutils.fs.cp("file:/tmp/clinicaltrial_2023.csv", "dbfs:/FileStore/tables/clinical_trials/clinicaltrial_2023.csv")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/clinical_trials/"))

# COMMAND ----------

# MAGIC %sh 
# MAGIC
# MAGIC rm /tmp/clinicaltrial_2023.csv

# COMMAND ----------

dbutils.fs.cp("FileStore/tables/pharma.zip", "file:/tmp/")

# COMMAND ----------

# MAGIC %sh 
# MAGIC unzip -d /tmp/  /tmp/pharma.zip

# COMMAND ----------

dbutils.fs.mkdirs("FileStore/tables/pharma/")

# COMMAND ----------

dbutils.fs.cp("file:/tmp/pharma.csv", "dbfs:/FileStore/tables/pharma/pharma.csv")

# COMMAND ----------

# MAGIC %sh 
# MAGIC
# MAGIC rm /tmp/pharma.csv

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/pharma/"))

# COMMAND ----------

from pyspark import SparkContext

clinicRDD = sc.textFile("/FileStore/tables/clinical_trials/clinicaltrial_2023.csv")

clinicRDD.take(2)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType


clinic_schema = StructType([
    StructField("Id", StringType(), True),
    StructField("Study Title", StringType(), True),
    StructField("Acronym", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Conditions", StringType(), True),
    StructField("Interventions", StringType(), True),
    StructField("Sponsor", StringType(), True),
    StructField("Collaborators", StringType(), True),
    StructField("Enrollment", IntegerType(), True),  
    StructField("Funder Type", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Study Design", StringType(), True),
    StructField("Start", StringType(), True), 
    StructField("Completion", StringType(), True)
])

# COMMAND ----------

from pyspark.sql import Row

number_of_columns = 14

def parse_line(line):
    parts = line.split('\t')
    if len(parts) != number_of_columns:
        return None
    try:
        parts[8] = int(parts[8]) if parts[8].isdigit() else None
    except ValueError:
        parts[8] = None
    return Row(*parts)

header = clinicRDD.first()
dataRDD = clinicRDD.filter(lambda row: row != header)

parsedRDD = dataRDD.map(parse_line).filter(lambda x: x is not None)

clinic_df = spark.createDataFrame(parsedRDD, clinic_schema)
clinic_df.show()
clinic_df.count()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

def remove_quotes(df):
    quotes_removed_df = df.withColumn("Id", regexp_replace("Id", '"', ''))
    quotes_removed_df = quotes_removed_df.withColumn("Completion", regexp_replace("Completion", '"', ''))
    return quotes_removed_df

clinic_df = remove_quotes(clinic_df)
clinic_df.show()

# COMMAND ----------

from pyspark.sql.functions import col, trim

def remove_trailing_punctuation(df):
    for column in df.columns:
        df = df.withColumn(column, trim(regexp_replace(col(column), ',+$', '')))
    return df

clinic_df = remove_trailing_punctuation(clinic_df)
clinic_df.show()

# COMMAND ----------

from pyspark.sql.functions import when

def fill_nulls(df):
    columns_to_fill = df.columns
    for column in columns_to_fill:
        df = df.withColumn(column, when(col(column) == '', None).otherwise(col(column)))
    return df

clinic_df = fill_nulls(clinic_df)
clinic_df.show()
clinic_df.count()

# COMMAND ----------

from pyspark.sql.functions import to_date, length

def normalise_dates(df, date_columns):
    for date in date_columns:
        df = df.withColumn(
            date,
            when(length(col(date)) == 7, to_date(col(date), 'yyyy-MM'))
            .otherwise(to_date(col(date), 'yyyy-MM-dd'))
        )
    return df

date_columns = ['Start', 'Completion']

clinic_df = normalise_dates(clinic_df, date_columns)
clinic_df.show(10)

# COMMAND ----------



# COMMAND ----------

pharma_df = spark.read.csv("/FileStore/tables/pharma/pharma.csv", header = True, inferSchema=True)

clinic_df.createOrReplaceTempView("clinic")

pharma_df.createOrReplaceTempView("pharma")

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM clinic
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pharma
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(DISTINCT Id) AS Number_of_Studies
# MAGIC FROM clinic

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   Type,
# MAGIC   COUNT(*) AS count
# MAGIC FROM clinic
# MAGIC   GROUP BY type
# MAGIC   ORDER BY count DESC
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   Condition,
# MAGIC   COUNT(*) AS count
# MAGIC FROM (
# MAGIC   SELECT EXPLODE(SPLIT(Conditions, ';')) AS Condition
# MAGIC   FROM clinic
# MAGIC )
# MAGIC WHERE Condition != 'Healthy'
# MAGIC GROUP BY Condition
# MAGIC ORDER BY count DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   Sponsor,
# MAGIC   COUNT(*) AS count
# MAGIC FROM clinic
# MAGIC WHERE Sponsor NOT IN (
# MAGIC     SELECT DISTINCT Parent_Company FROM pharma
# MAGIC )
# MAGIC GROUP BY Sponsor
# MAGIC ORDER BY count DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   MONTH(Completion) AS Month,
# MAGIC   COUNT(*) AS count
# MAGIC FROM clinic
# MAGIC WHERE YEAR(Completion) = 2023
# MAGIC AND Status = 'COMPLETED'
# MAGIC GROUP BY MONTH(Completion)
# MAGIC ORDER BY Month
