# Databricks notebook source
workers_22 = spark.table("dissertation.default.filtered_data_2022")

# COMMAND ----------

workers_23 = spark.table("dissertation.default.filtered_data_2023")

# COMMAND ----------

from pyspark.sql.functions import when, col

def replace_invalid_values_with_null(df):
    for column in df.columns:
        df = df.withColumn(column, when((col(column) == "") | (col(column) == "-1"), None).otherwise(col(column)))
    return df

workers_22 = replace_invalid_values_with_null(workers_22)
workers_23 = replace_invalid_values_with_null(workers_23)

# COMMAND ----------

workers_22.show()

# COMMAND ----------

workers_23.show()

# COMMAND ----------

from pyspark.sql.functions import col

new_staff = workers_23.join(workers_22, workers_23["worker_id"] == workers_22["worker_id"], "left_anti")

new_staff_pd = new_staff.toPandas()
print(new_staff_pd)

# COMMAND ----------

from pyspark.sql.functions import col, coalesce

worker_data = workers_22.join(workers_23, "worker_id", "full_outer")

worker_data = worker_data.select(
    coalesce(workers_22["worker_id"], workers_23["worker_id"]).alias("worker_id"),
    coalesce(workers_22["workplace_sector"], workers_23["workplace_sector"]).alias("workplace_sector"),
    coalesce(workers_22["main_service"], workers_23["main_service"]).alias("main_service"),
    coalesce(workers_22["main_service_gr"], workers_23["main_service_gr"]).alias("main_service_gr"),
    coalesce(workers_22["region"], workers_23["region"]).alias("region"),
    coalesce(workers_22["main_job_role"], workers_23["main_job_role"]).alias("main_job_role"),
    coalesce(workers_22["start_date"], workers_23["start_date"]).alias("start_date"),
    coalesce(workers_22["startdateyr_gr"], workers_23["startdateyr_gr"]).alias("startdateyr_gr"),
    coalesce(workers_22["age"], workers_23["age"]).alias("age"),
    coalesce(workers_22["age_gr"], workers_23["age_gr"]).alias("age_gr"),
    coalesce(workers_22["gender"], workers_23["gender"]).alias("gender"),
    coalesce(workers_22["disabled"], workers_23["disabled"]).alias("disabled"),
    coalesce(workers_22["eea_born"], workers_23["eea_born"]).alias("eea_born"),
    coalesce(workers_22["distance_from_work_miles"], workers_23["distance_from_work_miles"]).alias("distance_from_work_miles"),
    coalesce(workers_22["source_of_recruitment"], workers_23["source_of_recruitment"]).alias("source_of_recruitment"),
    coalesce(workers_22["days_sick_gr"], workers_23["days_sick_gr"]).alias("days_sick_gr"),
    coalesce(workers_22["zerohours"], workers_23["zerohours"]).alias("zerohours"),
    coalesce(workers_22["contract_type"], workers_23["contract_type"]).alias("contract_type"),
    coalesce(workers_22["avghrs"], workers_23["avghrs"]).alias("avghrs"),
    coalesce(workers_22["avghrs_gr"], workers_23["avghrs_gr"]).alias("avghrs_gr"),
    coalesce(workers_22["contrhrs"], workers_23["contrhrs"]).alias("contrhrs"),
    coalesce(workers_22["contrhrs_gr"], workers_23["contrhrs_gr"]).alias("contrhrs_gr"),
    coalesce(workers_22["hourly_rate"], workers_23["hourly_rate"]).alias("hourly_rate"),
    coalesce(workers_22["fte_pay"], workers_23["fte_pay"]).alias("fte_pay"),
    coalesce(workers_22["salary"], workers_23["salary"]).alias("salary"),
    coalesce(workers_22["salary_type"], workers_23["salary_type"]).alias("salary_type"),
    coalesce(workers_22["apprentice"], workers_23["apprentice"]).alias("apprentice"),
    coalesce(workers_22["holds_scq"], workers_23["holds_scq"]).alias("holds_scq"),
    coalesce(workers_22["scq_level"], workers_23["scq_level"]).alias("scq_level"),
    coalesce(workers_22["holds_qual"], workers_23["holds_qual"]).alias("holds_qual"),
    coalesce(workers_22["highest_level_qualification"], workers_23["highest_level_qualification"]).alias("highest_level_qualification"),
    coalesce(workers_22["hsc_nvq1"], workers_23["hsc_nvq1"]).alias("hsc_nvq1"),
    coalesce(workers_22["hsc_nvq2"], workers_23["hsc_nvq2"]).alias("hsc_nvq2"),
    coalesce(workers_22["hsc_nvq3"], workers_23["hsc_nvq3"]).alias("hsc_nvq3"),
    coalesce(workers_22["care_nvq1"], workers_23["care_nvq1"]).alias("care_nvq1"),
    coalesce(workers_22["care_nvq2"], workers_23["care_nvq2"]).alias("care_nvq2"),
    coalesce(workers_22["care_nvq3"], workers_23["care_nvq3"]).alias("care_nvq3"),
    coalesce(workers_22["other_nvq"], workers_23["other_nvq"]).alias("other_nvq"),
    when(workers_22["worker_id"].isNull(), "New")
        .when(workers_23["worker_id"].isNull(), "Left")
        .otherwise("Employed").alias("employment_status")
)

worker_data.show(truncate=False)



# COMMAND ----------

from pyspark.sql.functions import col, when, trim

worker_data = worker_data.withColumn(
    "salary",
    when(trim(col("salary")) == "", None).otherwise(trim(col("salary")))
)

worker_data.select("salary").show(truncate=False)


# COMMAND ----------

display(worker_data)

# COMMAND ----------

worker_data.describe()
worker_data.printSchema()

# COMMAND ----------


from pyspark.sql.functions import when, col

worker_data = worker_data.withColumn(
    "zerohours",
    when(col("zerohours").isin(-2, -1), 0)
    .otherwise(col("zerohours"))
)

worker_data.select("zerohours").show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import when, col

worker_data = worker_data.withColumn(
    "contract_type",
    when(col("zerohours") == 1, 3).otherwise(col("contract_type"))
)

worker_data.select("zerohours", "contract_type").show(truncate=False)


# COMMAND ----------

worker_data.show(2000, truncate=False)

# COMMAND ----------

worker_data = worker_data.drop("zerohours")
worker_data.show()

# COMMAND ----------

from pyspark.sql.functions import coalesce

worker_data = worker_data.withColumn(
    "weekly_hours",
    coalesce(col("avghrs"), col("contrhrs")).cast("integer")
)

worker_data = worker_data.withColumn(
    "hours_gr",
    coalesce(col("avghrs_gr"), col("contrhrs_gr")).cast("integer")
)

worker_data = worker_data.drop("avghrs", "avghrs_gr", "contrhrs", "contrhrs_gr")

worker_data.show()


# COMMAND ----------

from pyspark.sql.functions import col

all_columns = worker_data.columns

all_columns = [c for c in all_columns if c not in ['weekly_hours', 'hours_gr']]

all_columns.insert(17, 'weekly_hours')
all_columns.insert(18, 'hours_gr')

worker_data = worker_data.select([col(c) for c in all_columns])

worker_data.show(truncate=False)


# COMMAND ----------

from pyspark.sql import DataFrame

worker_data = worker_data.drop("salary", "fte_pay")

display(worker_data)

# COMMAND ----------

worker_data = worker_data.withColumn(
    "weekly_hours",
    when(col("weekly_hours") == -2, -1).otherwise(col("weekly_hours"))
)

worker_data = worker_data.withColumn(
    "apprentice",
    when(col("apprentice") == 3, 2).otherwise(col("apprentice"))
)

worker_data = worker_data.withColumn(
    "holds_scq",
    when(col("holds_scq") == 3, 2).otherwise(col("holds_scq"))
)

worker_data = worker_data.withColumn(
    "disabled",
    when(col("disabled") == -2, 2).otherwise(col("disabled"))
)

# COMMAND ----------

worker_data = worker_data.drop("main_service_gr")


# COMMAND ----------

worker_data.select("contract_type", "workplace_sector", "disabled", "gender", "apprentice").distinct().show(truncate=False)

# COMMAND ----------

worker_data = worker_data.withColumn(
    "contract_type", when(trim(col("contract_type")) == "", None).otherwise(col("contract_type"))
)
worker_data = worker_data.withColumn(
    "workplace_sector", when(trim(col("workplace_sector")) == "", None).otherwise(col("workplace_sector"))
)
worker_data = worker_data.withColumn(
    "disabled", when(trim(col("disabled")) == "", None).otherwise(col("disabled"))
)
worker_data = worker_data.withColumn(
    "gender", when(trim(col("gender")) == "", None).otherwise(col("gender"))
)

worker_data.select("contract_type", "workplace_sector", "disabled", "gender").distinct().show(truncate=False)

# COMMAND ----------

worker_data = worker_data.withColumn(
    "apprentice",
    when(col("apprentice") == 3, 2).otherwise(col("apprentice"))
)

worker_data.select("apprentice").distinct().show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import count, isnull

for column in worker_data.columns:
    null_count = worker_data.filter(col(column).isNull()).count()
    if null_count > 0:
        print(f"Column {column} has {null_count} null values")
    else:
        print(f"Column {column} has no null values")

# COMMAND ----------

worker_data = worker_data.fillna({
  'disabled': '0',
  'source_of_recruitment': '-1',
  'contract_type': '3',
  'gender': '3',
  'apprentice': '2', 
  'holds_scq': '2'
})

# COMMAND ----------

from pyspark.sql.functions import col, count, max as max_
from pyspark.sql.window import Window

def calculate_mode(df, column_name):
    mode_df = df.groupBy(column_name).agg(count(column_name).alias("count"))
    window = Window.orderBy(col("count").desc())
    mode_value = mode_df.withColumn("max_count", max_("count").over(window)) \
                        .where(col("count") == col("max_count")) \
                        .select(column_name).first()[column_name]
    return mode_value

mode_workplace_sector = calculate_mode(worker_data, "workplace_sector")

mode_source_of_recruitment = calculate_mode(worker_data, "source_of_recruitment")

# COMMAND ----------

worker_data = worker_data.na.fill({
    "workplace_sector": mode_workplace_sector,
    "source_of_recruitment": mode_source_of_recruitment
})

# COMMAND ----------

for column in worker_data.columns:
    null_count = worker_data.filter(col(column).isNull()).count()
    if null_count > 0:
        print(f"Column {column} has {null_count} null values")
    else:
        print(f"Column {column} has no null values")

# COMMAND ----------

from pyspark.sql.functions import col

median_age = worker_data.approxQuantile("age", [0.5], 0.01)[0]

worker_data = worker_data.na.fill({"age": int(median_age)})

# COMMAND ----------

def age_to_age_gr(age):
  if age < 18:
    return 270
  elif age < 20:
    return 271
  elif age < 25:
    return 272
  elif age < 30:
    return 273
  elif age < 35:
    return 274
  elif age < 40:
    return 275
  elif age < 45:
    return 276
  elif age < 50:
    return 277
  elif age < 55:
    return 278
  elif age < 60:
    return 279
  elif age < 65:
    return 280
  elif age < 70:
    return 281
  else:
    return 282

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

age_group_udf = udf(age_to_age_gr, IntegerType())

# COMMAND ----------

worker_data = worker_data.withColumn(
    "age_gr", 
    when(col("age_gr").isNull(), age_group_udf(col("age"))).otherwise(col("age_gr"))
)

# COMMAND ----------

from pyspark.sql.functions import col

columns_to_check = ['main_service', 'region']

unique_values_per_column = []

for column in columns_to_check:
    distinct_values = worker_data.select(column).distinct()
    
    collected_values = [row[column] for row in distinct_values.collect()]
    
    unique_values_per_column.append((column, collected_values))

for column, values in unique_values_per_column:
    print(f"Unique values in {column}: {values}")

# COMMAND ----------

from pyspark.sql.functions import col

worker_data = worker_data.withColumn("weekly_hours", col("weekly_hours").cast("integer"))

# COMMAND ----------

from pyspark.sql import functions as F

frequent_hours = worker_data.groupBy("weekly_hours").count().orderBy(F.desc("count"))

filtered_hours = frequent_hours.filter(frequent_hours["weekly_hours"] != -1)

if filtered_hours.count() > 0:
    mode_hours = filtered_hours.first()["weekly_hours"]
    worker_data = worker_data.na.fill({"weekly_hours": mode_hours})

# COMMAND ----------

def weekly_hours_to_hours_gr(weekly_hours):
  if weekly_hours == 0:
    return 0
  elif weekly_hours < 10:
    return 1
  elif weekly_hours < 11:
    return 2
  elif weekly_hours < 16:
    return 3
  elif weekly_hours < 20:
    return 4
  elif weekly_hours < 21:
    return 5
  elif weekly_hours < 26:
    return 6
  elif weekly_hours < 30:
    return 7
  elif weekly_hours < 31:
    return 8
  elif weekly_hours < 35:
    return 9
  elif weekly_hours < 36:
    return 10
  elif weekly_hours < 37:
    return 11
  elif weekly_hours < 38:
    return 12
  elif weekly_hours < 39:
    return 13
  elif weekly_hours < 40:
    return 14
  elif weekly_hours < 41:
    return 15
  elif weekly_hours < 42:
    return 16
  elif weekly_hours < 43:
    return 17
  elif weekly_hours < 44:
    return 18
  elif weekly_hours < 45:
    return 19
  elif weekly_hours < 46:
    return 20
  else:
    return 21

# COMMAND ----------

hours_gr_udf = udf(weekly_hours_to_hours_gr, IntegerType())

worker_data = worker_data.withColumn(
    "hours_gr", 
    when(col("hours_gr").isNull(), hours_gr_udf(col("weekly_hours"))).otherwise(col("hours_gr"))
)

# COMMAND ----------

from pyspark.sql.functions import mean

mean_distance = worker_data.select(mean(col("distance_from_work_miles")).alias("mean_distance")).collect()[0]["mean_distance"]

worker_data = worker_data.na.fill({"distance_from_work_miles": mean_distance})

# COMMAND ----------

worker_data = worker_data.na.fill({"scq_level": "0", "highest_level_qualification": "0"})

# COMMAND ----------

for column in worker_data.columns:
    null_count = worker_data.filter(col(column).isNull()).count()
    if null_count > 0:
        print(f"Column {column} has {null_count} null values")
    else:
        print(f"Column {column} has no null values")

# COMMAND ----------

worker_data = worker_data.withColumn(
    "employment_status",
    when(col("employment_status") == "Left", 0)
    .when(col("employment_status") == "Employed", 1)
    .when(col("employment_status") == "New", 2)
    )

display(worker_data)

# COMMAND ----------


from pyspark.sql.functions import when, col, lit

worker_data = worker_data.withColumn(
    "workplace_sector",
    when(col("workplace_sector") == 1, "Local authority")
    .when(col("workplace_sector") == 2, "Independent")
    .when(col("workplace_sector") == 3, "Voluntary / Charity")
    .when(col("workplace_sector") == 4, "Other")
    .when(col("workplace_sector") == -1, "Other") 
)

display(worker_data)

# COMMAND ----------

worker_data = worker_data.withColumn(
    "main_service",
    when(col("main_service") == 8, "Domiciliary Care")
    .when(col("main_service") == 10, "Domestic Services and Home Help")
    .when(col("main_service") == 12, "Other Domiciliary Care Services")
    .when(col("main_service") == 54, "Extra Care Housing")
    .when(col("main_service") == 55, "Supported Living")
    .otherwise(lit(None)) 
)

display(worker_data)

# COMMAND ----------

worker_data = worker_data.withColumn(
    "region",
    when(col("region") == 1, "Eastern")
    .when(col("region") == 2, "East Midlands")
    .when(col("region") == 3, "London")
    .when(col("region") == 4, "North East")
    .when(col("region") == 5, "North West")
    .when(col("region") == 6, "South East")
    .when(col("region") == 7, "South West")
    .when(col("region") == 8, "West Midlands")
    .when(col("region") == 9, "Yorkshire and the Humber")    
)

display(worker_data)

# COMMAND ----------

worker_data = worker_data.withColumn("main_job_role", col("main_job_role").cast("int"))

worker_data = worker_data.withColumn(
    "main_job_role",
    when(col("main_job_role") == 7, "Senior Care Worker")
    .when(col("main_job_role") == 8, "Care Worker")
)

display(worker_data)

# COMMAND ----------

worker_data = worker_data.withColumn(
    "gender",
    when(col("gender") == 1, "Male")
    .when(col("gender") == 2, "Female")
    .when(col("gender") == 3, "Unknown")
    .when(col("gender") == 4, "Other")
)

display(worker_data)

# COMMAND ----------

worker_data = worker_data.withColumn(
    "eea_born",
    when(col("eea_born") == 0, "Non-EEA")
    .when(col("eea_born") == 1, "EEA (non_UK)")
    .when(col("eea_born") == 2, "UK")
    .when(col("eea_born") == 3, "Not recorded")
    .when(col("eea_born") == 4, "Non-UK (Not known)")
)

display(worker_data)

# COMMAND ----------

worker_data = worker_data.withColumn(
    "disabled",
    when(col("disabled") == 0, "No")
    .when(col("disabled") == 1, "Yes")
    .when(col("disabled") == 2, "Not Disclosed")
)

display(worker_data)

# COMMAND ----------

columns_to_check = ['days_sick_gr', 'contract_type', 'salary_type']

unique_values_per_column = []

for column in columns_to_check:
    distinct_values = worker_data.select(column).distinct()
    
    collected_values = [row[column] for row in distinct_values.collect()]
    
    unique_values_per_column.append((column, collected_values))

for column, values in unique_values_per_column:
    print(f"Unique values in {column}: {values}")

# COMMAND ----------

worker_data = worker_data.withColumn(
    "contract_type",
    when(col("contract_type") == 1, "Full-Time")
    .when(col("contract_type") == 2, "Part-Time")
    .when(col("contract_type") == 3, "Zero Hour")
)

worker_data = worker_data.withColumn(
    "weekly_hours",
    when(col("weekly_hours") == -1, "Unknown")
    .otherwise(col("weekly_hours"))
)

worker_data = worker_data.withColumn(
    "hours_gr",
    when(col("hours_gr") == -1, "Unknown")
    .otherwise(col("hours_gr"))
)

mode = worker_data.filter(col("hours_gr") != -2) \
                  .groupBy("hours_gr") \
                  .count() \
                  .orderBy("count", ascending=False) \
                  .first()[0]

worker_data = worker_data.withColumn(
    "hours_gr",
    when(col("hours_gr") == -2, mode)
    .otherwise(col("hours_gr"))
)

display(worker_data)

# COMMAND ----------

null_startdateyr_gr = worker_data.filter(col("startdateyr_gr").isNull())

display(null_startdateyr_gr)

# COMMAND ----------

worker_data = worker_data.withColumn(
    "startdateyr_gr",
    when(col("startdateyr_gr").isNull(), 320).otherwise(col("startdateyr_gr"))
)

# COMMAND ----------

for column in worker_data.columns:
    null_count = worker_data.filter(col(column).isNull()).count()
    if null_count > 0:
        print(f"Column {column} has {null_count} null values")
    else:
        print(f"Column {column} has no null values")

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

staff_data_pd = staff_data_predictions.select("age", "distance_from_work_miles").toPandas()

plt.figure(figsize=(12, 6))
sns.histplot(staff_data_pd['age'], kde=True, bins=100)
plt.title("Distribution of Age")
plt.xlabel("Age")
plt.ylabel("Frequency")
plt.show()

plt.figure(figsize=(12, 6))
sns.histplot(staff_data_pd['distance_from_work_miles'], kde=True, bins=100)
plt.title("Distribution of Distance from Work (Miles)")
plt.xlabel("Distance from Work (Miles)")
plt.ylabel("Frequency")
plt.show()

# COMMAND ----------

worker_data_pd = worker_data.select('hourly_rate').toPandas()

display(worker_data_pd)


# COMMAND ----------

from pyspark.sql.functions import expr

percentiles = worker_data.approxQuantile("hourly_rate", [0.02, 0.98], 0.01)
lower_bound = percentiles[0]
upper_bound = percentiles[1]

filtered_data = worker_data.filter(expr(f"hourly_rate >= {lower_bound} AND hourly_rate <= {upper_bound}"))

# COMMAND ----------

filtered_data.pd = filtered_data.select('hourly_rate').toPandas()

display(filtered_data.pd)

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

age_salary_df = filtered_data.select("age", "hourly_rate").toPandas()

sns.pairplot(age_salary_df, diag_kind='kde', height=2.5)
plt.suptitle('Pair Plot for Age and Hourly Rate', y=1.02) 
plt.show()

# COMMAND ----------

import seaborn as sns

qual_salary_df = filtered_data.select("holds_qual", "hourly_rate").toPandas()

plt.figure(figsize=(12, 8))
sns.violinplot(x='holds_qual', y='hourly_rate', data=qual_salary_df, inner="quartile", palette="Set2")

plt.xlabel('Holds Qualification')
plt.ylabel('Hourly Rate')
plt.title('Hourly Rate by Qualification')

plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

# Convert the Spark DataFrame to a Pandas DataFrame
age_contract_df = filtered_data.select("age_gr", "contract_type").dropna().toPandas()

# Create a mapping of the original age groups to the new labels
age_group_mapping = {
    270: "Under 18",
    271: "18 to 19",
    272: "20 to 24",
    273: "25 to 29",
    274: "30 to 34",
    275: "35 to 39",
    276: "40 to 44",
    277: "45 to 49",
    278: "50 to 54",
    279: "55 to 59",
    280: "60 to 64",
    281: "65 to 69",
    282: "70 and above"
}

age_contract_df['age_gr'] = age_contract_df['age_gr'].map(age_group_mapping)

age_group_order = [
    "Under 18", "18 to 19", "20 to 24", "25 to 29", "30 to 34", 
    "35 to 39", "40 to 44", "45 to 49", "50 to 54", "55 to 59", 
    "60 to 64", "65 to 69", "70 and above"
]

age_contract_crosstab = pd.crosstab(age_contract_df['age_gr'], age_contract_df['contract_type'])

age_contract_crosstab = age_contract_crosstab.reindex(age_group_order)

age_contract_crosstab.plot(kind='bar', stacked=True)
plt.xlabel('Age Group')
plt.ylabel('Count')
plt.title('Contract Type Distribution Across Age Groups')
plt.xticks(rotation=45, ha='right')
plt.show()


# COMMAND ----------

turnover_data = filtered_data.filter(
    (col("employment_status") == "0") | (col("employment_status") == "1")
)

display(turnover_data)

# COMMAND ----------

status_counts = turnover_data.groupBy("employment_status").count()

status_counts.show()

# COMMAND ----------

status_counts = turnover_data.groupBy("employment_status").count().collect()

left_count = [row['count'] for row in status_counts if row['employment_status'] == 0][0]
employed_count = [row['count'] for row in status_counts if row['employment_status'] == 1][0]

total_count = left_count + employed_count

left_percentage = (left_count / total_count) * 100
employed_percentage = (employed_count / total_count) * 100

print(f"Employed: {employed_count} ({employed_percentage:.2f}%)")
print(f"Left: {left_count} ({left_percentage:.2f}%)")

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml import Pipeline

workplace_sector_indexer = StringIndexer(inputCol="workplace_sector", outputCol="workplace_sector_index")
main_service_indexer = StringIndexer(inputCol="main_service", outputCol="main_service_index")
main_job_role_indexer = StringIndexer(inputCol="main_job_role", outputCol="main_job_role_index")
region_indexer = StringIndexer(inputCol="region", outputCol="region_index")
gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_index")
disabled_indexer = StringIndexer(inputCol="disabled", outputCol="disabled_index")
eea_born_indexer = StringIndexer(inputCol="eea_born", outputCol="eea_born_index")
source_of_recruitment_indexer = StringIndexer(inputCol="source_of_recruitment", outputCol="source_of_recruitment_index")
contract_type_indexer = StringIndexer(inputCol="contract_type", outputCol="contract_type_index")
holds_scq_indexer = StringIndexer(inputCol="holds_scq", outputCol="holds_scq_index")

workplace_sector_encoder = OneHotEncoder(inputCol="workplace_sector_index", outputCol="workplace_sector_vec")
main_service_encoder = OneHotEncoder(inputCol="main_service_index", outputCol="main_service_vec")
main_job_role_encoder = OneHotEncoder(inputCol="main_job_role_index", outputCol="main_job_role_vec")
region_encoder = OneHotEncoder(inputCol="region_index", outputCol="region_vec")
gender_encoder = OneHotEncoder(inputCol="gender_index", outputCol="gender_vec")
disabled_encoder = OneHotEncoder(inputCol="disabled_index", outputCol="disabled_vec")
eea_born_encoder = OneHotEncoder(inputCol="eea_born_index", outputCol="eea_born_vec")
source_of_recruitment_encoder = OneHotEncoder(inputCol="source_of_recruitment_index", outputCol="source_of_recruitment_vec")
contract_type_encoder = OneHotEncoder(inputCol="contract_type_index", outputCol="contract_type_vec")
holds_scq_encoder = OneHotEncoder(inputCol="holds_scq_index", outputCol="holds_scq_vec")

pipeline = Pipeline(stages=[
    workplace_sector_indexer, main_service_indexer, main_job_role_indexer,
    region_indexer, gender_indexer, eea_born_indexer, source_of_recruitment_indexer, 
    contract_type_indexer, holds_scq_indexer, disabled_indexer,
    workplace_sector_encoder, main_service_encoder, main_job_role_encoder,
    region_encoder, gender_encoder, eea_born_encoder, source_of_recruitment_encoder, 
    contract_type_encoder, holds_scq_encoder, disabled_encoder
])

turnover_data = pipeline.fit(turnover_data).transform(turnover_data)

display(turnover_data)


# COMMAND ----------

turnover_data = turnover_data.withColumn("startdateyr_gr", col("startdateyr_gr").cast("int"))
turnover_data = turnover_data.withColumn("hours_gr", col("hours_gr").cast("int"))
turnover_data = turnover_data.withColumn("salary_type", col("salary_type").cast("int"))
turnover_data = turnover_data.withColumn("apprentice", col("apprentice").cast("int"))
turnover_data = turnover_data.withColumn("scq_level", col("scq_level").cast("int"))
turnover_data = turnover_data.withColumn("holds_qual", col("holds_qual").cast("int"))
turnover_data = turnover_data.withColumn("highest_level_qualification", col("highest_level_qualification").cast("int"))

# COMMAND ----------

for column in turnover_data.columns:
    null_count = turnover_data.filter(col(column).isNull()).count()
    if null_count > 0:
        print(f"Column {column} has {null_count} null values")
    else:
        print(f"Column {column} has no null values")

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

feature_columns = [
    "startdateyr_gr", "age_gr", "distance_from_work_miles", "days_sick_gr", "hours_gr", "salary_type", "apprentice", "scq_level", "holds_qual", "highest_level_qualification", "hsc_nvq1", "hsc_nvq2", "hsc_nvq3", "care_nvq1", "care_nvq2", "care_nvq3", "other_nvq", "workplace_sector_vec", "main_service_vec", "main_job_role_vec", "region_vec", "gender_vec", "eea_born_vec", "source_of_recruitment_vec", "contract_type_vec", "holds_scq_vec", "disabled_vec"
]

assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

turnover_data = assembler.transform(turnover_data)

turnover_data.select("features", "employment_status").show(truncate=False)


# COMMAND ----------

train_data, test_data = turnover_data.randomSplit([0.8, 0.2], seed=228)

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(featuresCol="features", labelCol="employment_status", maxIter=10)

# COMMAND ----------

lr_model = lr.fit(train_data)

print("Model trained")

# COMMAND ----------

lr_predictions = lr_model.transform(test_data)

lr_predictions.select("features", "employment_status", "prediction").show(truncate=False)

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

acc_evaluator = MulticlassClassificationEvaluator(labelCol="employment_status", metricName="accuracy")
auc_evaluator = BinaryClassificationEvaluator(labelCol="employment_status", metricName="areaUnderROC")

lr_accuracy = acc_evaluator.evaluate(lr_predictions)
lr_auc = auc_evaluator.evaluate(lr_predictions)

print("Test Accuracy = {}".format(lr_accuracy))
print("Test AUC = {}".format(lr_auc))

# COMMAND ----------

lr_predictions = lr_predictions.withColumn("prediction_label", col("prediction").cast("int"))

confusion_matrix = lr_predictions.groupBy("employment_status", "prediction_label").count().show()

# COMMAND ----------

confusion_matrix_lr = lr_predictions.groupBy("employment_status", "prediction_label").count().toPandas()

confusion_matrix_pivot = confusion_matrix_lr.pivot(index="employment_status", columns="prediction_label", values="count").fillna(0)


# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

plt.figure(figsize=(8, 6))
sns.heatmap(confusion_matrix_pivot, annot=True, fmt="g", cmap="Blues", xticklabels=["Left", "Employed"], yticklabels=["Left", "Employed"])

plt.ylabel('Actual')
plt.xlabel('Predicted')
plt.title('Confusion Matrix')
plt.show()

# COMMAND ----------

import numpy as np

features = assembler.getInputCols()

coefficients = np.abs(lr_model.coefficients.toArray())

print(f"Number of features: {len(features)}")
print(f"Number of coefficients: {len(coefficients)}")

# COMMAND ----------

expanded_features = []

coeff_index = 0

for feature in features:
    if feature.endswith("_vec"):
        categories = turnover_data.select(feature).head()[0].size
        for i in range(categories):
            expanded_features.append(f"{feature}_{i}")
            coeff_index += 1
    else:
        expanded_features.append(feature)
        coeff_index += 1

print(f"Number of expanded features: {len(expanded_features)}")
print(f"Number of coefficients: {len(coefficients)}")

plt.figure(figsize=(12, 8))
sns.barplot(x=expanded_features, y=coefficients)
plt.xticks(rotation=90)
plt.title('Feature Importance (Logistic Regression Coefficients)')
plt.show()


# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

f1_evaluator = MulticlassClassificationEvaluator(
    labelCol="employment_status", predictionCol="prediction", metricName="f1"
)
lr_f1 = f1_evaluator.evaluate(lr_predictions)
print(f"LR F1 Score: {lr_f1}")

tp = lr_predictions.filter((col("employment_status") == 1) & (col("prediction") == 1)).count()
fp = lr_predictions.filter((col("employment_status") == 0) & (col("prediction") == 1)).count()
fn = lr_predictions.filter((col("employment_status") == 1) & (col("prediction") == 0)).count()

lr_precision = tp / (tp + fp) if (tp + fp) != 0 else 0
lr_recall = tp / (tp + fn) if (tp + fn) != 0 else 0

print(f"LR Precision: {lr_precision}")
print(f"LR Recall: {lr_recall}")

# COMMAND ----------

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator

lr = LogisticRegression(featuresCol='features', labelCol='employment_status')

paramGrid = (ParamGridBuilder()
             .addGrid(lr.regParam, [0.01, 0.1, 1.0]) 
             .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) 
             .build())

evaluator = BinaryClassificationEvaluator(labelCol="employment_status")

crossval_lr = CrossValidator(estimator=lr,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator,
                          numFolds=5) 

cvModel_lr = crossval_lr.fit(train_data)

best_lr_predictions = cvModel_lr.transform(test_data)

# COMMAND ----------

lr_bestModel = cvModel_lr.bestModel

lr_best_model_predictions = lr_bestModel.transform(test_data)

lr_test_accuracy = acc_evaluator.evaluate(lr_best_model_predictions)
lr_test_auc = auc_evaluator.evaluate(lr_best_model_predictions)

print(f"Best Regularization Parameter: {lr_bestModel._java_obj.getRegParam()}")
print(f"Best ElasticNet Parameter: {lr_bestModel._java_obj.getElasticNetParam()}")
print(f"Best Model Test Accuracy: {lr_test_accuracy}")
print(f"Best Model Test AUC-ROC: {lr_test_auc}")

# COMMAND ----------

best_lr_f1 = f1_evaluator.evaluate(lr_best_model_predictions)
print(f"LR F1 Score: {lr_f1}")

tp = lr_best_model_predictions.filter((col("employment_status") == 1) & (col("prediction") == 1)).count()
fp = lr_best_model_predictions.filter((col("employment_status") == 0) & (col("prediction") == 1)).count()
fn = lr_best_model_predictions.filter((col("employment_status") == 1) & (col("prediction") == 0)).count()

best_lr_precision = tp / (tp + fp) if (tp + fp) != 0 else 0
best_lr_recall = tp / (tp + fn) if (tp + fn) != 0 else 0

print(f"LR Precision: {best_lr_precision}")
print(f"LR Recall: {best_lr_recall}")

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from sklearn.metrics import roc_curve, auc
import matplotlib.pyplot as plt
import pandas as pd

preds_and_labels = best_lr_predictions.select("probability", "employment_status").rdd.map(lambda row: (float(row['probability'][1]), float(row['employment_status'])))

preds_and_labels_df = preds_and_labels.toDF(['probability', 'label']).toPandas()

fpr, tpr, thresholds = roc_curve(preds_and_labels_df['label'], preds_and_labels_df['probability'])
roc_auc = auc(fpr, tpr)

plt.figure(figsize=(10, 6))
plt.plot(fpr, tpr, color='darkorange', lw=2, label='ROC curve (area = %0.2f)' % roc_auc)
plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('Receiver Operating Characteristic for Logistic Regression')
plt.legend(loc="lower right")
plt.show()


# COMMAND ----------

lr_predictions.select("probability").printSchema()

# COMMAND ----------

from pyspark.ml.linalg import DenseVector

def extract_prob(v):
    if isinstance(v, DenseVector):
        return float(v[1])
    else:
        return None

from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

extract_prob_udf = udf(extract_prob, FloatType())

lr_predictions = lr_predictions.withColumn("probability_class1", extract_prob_udf(col("probability")))

lr_predictions = lr_predictions.withColumn("error", col("probability_class1") - col("employment_status"))

lr_predictions.select("probability_class1", "employment_status", "error").show(truncate=False)

# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

lr_features_errors = lr_predictions.select("features", "error").rdd.map(lambda row: row.features.toArray().tolist() + [row.error]).collect()
lr_df = pd.DataFrame(lr_features_errors, columns=expanded_features + ["error"])

lr_correlation = lr_df.corr()["error"].drop("error")

plt.figure(figsize=(10, 8))
sns.barplot(x=lr_correlation.index, y=lr_correlation.values, palette="Blues_d")
plt.xticks(rotation=90)
plt.title('Correlation between Features and Errors (Logistic Regression)')
plt.xlabel('Features')
plt.ylabel('Correlation with Error')
plt.show()



# COMMAND ----------

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator 

rf = RandomForestClassifier(featuresCol='features', labelCol='employment_status')

paramGrid = (ParamGridBuilder()
             .addGrid(rf.numTrees, [100, 200, 300])
             .addGrid(rf.maxDepth, [5, 10, 15])
             .addGrid(rf.maxBins, [32])
             .addGrid(rf.minInstancesPerNode, [1, 3])
             .build())

evaluator_auc = BinaryClassificationEvaluator(labelCol="employment_status")
 
crossval_rf = CrossValidator(estimator=rf,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator,
                          numFolds=5)

cvModel_rf = crossval_rf.fit(train_data)

rf_predictions = cvModel_rf.transform(test_data)

# COMMAND ----------

rf_best_model = cvModel_rf.bestModel

print("Best Parameters:")
print(f"numTrees={rf_best_model.getNumTrees}")
print(f"maxDepth={rf_best_model.getMaxDepth()}")
print(f"minInstancesPerNode={rf_best_model.getMinInstancesPerNode()}")

rf_accuracy = acc_evaluator.evaluate(rf_predictions)
rf_auc = auc_evaluator.evaluate(rf_predictions)

print(f"Test Accuracy: {rf_accuracy}")
print(f"Test AUC-ROC: {rf_auc}")

# COMMAND ----------

confusion_matrix_rf = rf_predictions.groupBy("employment_status", "prediction").count().toPandas()

confusion_matrix_pivot_rf = confusion_matrix_rf.pivot(index="employment_status", columns="prediction", values="count")

plt.figure(figsize=(8, 6))
sns.heatmap(confusion_matrix_pivot_rf, annot=True, fmt="g", cmap="Blues", xticklabels=["Left", "Employed"], yticklabels=["Left", "Employed"])

plt.ylabel('Actual')
plt.xlabel('Predicted')
plt.title('Confusion Matrix')
plt.show()

# COMMAND ----------

rf_f1 = f1_evaluator.evaluate(rf_predictions)
print(f"RF F1 Score: {rf_f1}")

tp = rf_predictions.filter((col("employment_status") == 1) & (col("prediction") == 1)).count()
fp = rf_predictions.filter((col("employment_status") == 0) & (col("prediction") == 1)).count()
fn = rf_predictions.filter((col("employment_status") == 1) & (col("prediction") == 0)).count()

rf_precision = tp / (tp + fp) if (tp + fp) != 0 else 0
rf_recall = tp / (tp + fn) if (tp + fn) != 0 else 0

print(f"RF Precision: {rf_precision}")
print(f"RF Recall: {rf_recall}")

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from sklearn.metrics import roc_curve, auc
import matplotlib.pyplot as plt
import pandas as pd

preds_and_labels = rf_predictions.select("probability", "employment_status").rdd.map(lambda row: (float(row['probability'][1]), float(row['employment_status'])))

preds_and_labels_df = preds_and_labels.toDF(['probability', 'label']).toPandas()

fpr, tpr, thresholds = roc_curve(preds_and_labels_df['label'], preds_and_labels_df['probability'])
roc_auc = auc(fpr, tpr)

plt.figure(figsize=(10, 6))
plt.plot(fpr, tpr, color='darkorange', lw=2, label='ROC curve (area = %0.2f)' % roc_auc)
plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('Receiver Operating Characteristic for Random Forest')
plt.legend(loc="lower right")
plt.show()


# COMMAND ----------

from pyspark.ml.linalg import DenseVector

def extract_rf_prob(v):
    if isinstance(v, DenseVector):
        return float(v[1])  
    else:
        return None

extract_rf_prob_udf = udf(extract_rf_prob, FloatType())

# COMMAND ----------

extract_rf_prob_udf = udf(extract_rf_prob, FloatType())

rf_predictions = rf_predictions.withColumn("probability_class1", extract_rf_prob_udf(col("probability")))

rf_predictions = rf_predictions.withColumn("error", col("probability_class1") - col("employment_status"))

rf_predictions.select("probability_class1", "employment_status", "error").show(truncate=False)

rf_features_errors = rf_predictions.select("features", "error").rdd.map(lambda row: row.features.toArray().tolist() + [row.error]).collect()
rf_df = pd.DataFrame(rf_features_errors, columns=expanded_features + ["error"])

# COMMAND ----------

rf_correlation = rf_df.corr()["error"].drop("error")

plt.figure(figsize=(10, 8))
sns.barplot(x=rf_correlation.index, y=rf_correlation.values, palette="Blues_d")
plt.xticks(rotation=90)
plt.title('Correlation between Features and Errors (Random Forest)')
plt.xlabel('Features')
plt.ylabel('Correlation with Error')
plt.show()

# COMMAND ----------

comparison_df = pd.DataFrame({
    "Metric": ["Accuracy", "AUC-ROC", "F1 Score", "Precision", "Recall"],
    "Logistic Regression": [lr_accuracy, lr_auc, lr_f1, lr_precision, lr_recall],
    "Random Forest": [rf_accuracy, rf_auc, rf_f1, rf_precision, rf_recall]
})

print(comparison_df)

# COMMAND ----------

misclassified = rf_predictions.filter(col("employment_status") != col("prediction"))
misclassified.select("features", "employment_status", "prediction").show(truncate=False)

# COMMAND ----------

rf_importances = rf_best_model.featureImportances
print("Feature Importances:", rf_importances)

# COMMAND ----------

rf_importances = rf_best_model.featureImportances.toArray()

rf_importances_df = pd.DataFrame({
    'feature': expanded_features, 
    'importance': rf_importances
})

rf_importances_df = rf_importances_df.sort_values(by='importance', ascending=False)

plt.figure(figsize=(12, 8))
sns.barplot(x=rf_importances_df['importance'], y=rf_importances_df['feature'], palette="viridis")
plt.title('Feature Importances (Random Forest)')
plt.xlabel('Importance')
plt.ylabel('Feature')
plt.show()

# COMMAND ----------

X_test = test_data.select("features").rdd.map(lambda row: row.features.toArray()).collect()
X_test = np.array(X_test)

# COMMAND ----------

coefficients = np.array(lr_model.coefficients.toArray())
intercept = lr_model.intercept

sklearn_lr_model = LogisticRegression()
sklearn_lr_model.coef_ = coefficients.reshape(1, -1)
sklearn_lr_model.intercept_ = np.array([intercept])

# COMMAND ----------

feature_array = np.array(test_data.select("features").rdd.map(lambda row: row.features.toArray()).collect())
X_test_pd = pd.DataFrame(feature_array, columns=expanded_features)

# COMMAND ----------

staff_data = filtered_data.filter(
    (col("employment_status") == "1") | (col("employment_status") == "2")
)

# COMMAND ----------

staff_data.printSchema()

# COMMAND ----------

workplace_sector_indexer = StringIndexer(inputCol="workplace_sector", outputCol="workplace_sector_index")
main_service_indexer = StringIndexer(inputCol="main_service", outputCol="main_service_index")
main_job_role_indexer = StringIndexer(inputCol="main_job_role", outputCol="main_job_role_index")
region_indexer = StringIndexer(inputCol="region", outputCol="region_index")
gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_index")
disabled_indexer = StringIndexer(inputCol="disabled", outputCol="disabled_index")
eea_born_indexer = StringIndexer(inputCol="eea_born", outputCol="eea_born_index")
source_of_recruitment_indexer = StringIndexer(inputCol="source_of_recruitment", outputCol="source_of_recruitment_index")
contract_type_indexer = StringIndexer(inputCol="contract_type", outputCol="contract_type_index")
holds_scq_indexer = StringIndexer(inputCol="holds_scq", outputCol="holds_scq_index")

workplace_sector_encoder = OneHotEncoder(inputCol="workplace_sector_index", outputCol="workplace_sector_vec")
main_service_encoder = OneHotEncoder(inputCol="main_service_index", outputCol="main_service_vec")
main_job_role_encoder = OneHotEncoder(inputCol="main_job_role_index", outputCol="main_job_role_vec")
region_encoder = OneHotEncoder(inputCol="region_index", outputCol="region_vec")
gender_encoder = OneHotEncoder(inputCol="gender_index", outputCol="gender_vec")
disabled_encoder = OneHotEncoder(inputCol="disabled_index", outputCol="disabled_vec")
eea_born_encoder = OneHotEncoder(inputCol="eea_born_index", outputCol="eea_born_vec")
source_of_recruitment_encoder = OneHotEncoder(inputCol="source_of_recruitment_index", outputCol="source_of_recruitment_vec")
contract_type_encoder = OneHotEncoder(inputCol="contract_type_index", outputCol="contract_type_vec")
holds_scq_encoder = OneHotEncoder(inputCol="holds_scq_index", outputCol="holds_scq_vec")

pipeline = Pipeline(stages=[
    workplace_sector_indexer, main_service_indexer, main_job_role_indexer,
    region_indexer, gender_indexer, eea_born_indexer, source_of_recruitment_indexer, 
    contract_type_indexer, holds_scq_indexer, disabled_indexer,
    workplace_sector_encoder, main_service_encoder, main_job_role_encoder,
    region_encoder, gender_encoder, eea_born_encoder, source_of_recruitment_encoder, 
    contract_type_encoder, holds_scq_encoder, disabled_encoder
])

staff_data = pipeline.fit(staff_data).transform(staff_data)

# COMMAND ----------

staff_data = staff_data.withColumn("startdateyr_gr", col("startdateyr_gr").cast("int"))
staff_data = staff_data.withColumn("hours_gr", col("hours_gr").cast("int"))
staff_data = staff_data.withColumn("salary_type", col("salary_type").cast("int"))
staff_data = staff_data.withColumn("apprentice", col("apprentice").cast("int"))
staff_data = staff_data.withColumn("scq_level", col("scq_level").cast("int"))
staff_data = staff_data.withColumn("holds_qual", col("holds_qual").cast("int"))
staff_data = staff_data.withColumn("highest_level_qualification", col("highest_level_qualification").cast("int"))

feature_columns = [
    "startdateyr_gr", "age_gr", "distance_from_work_miles", "days_sick_gr", "hours_gr", "salary_type", "apprentice", "scq_level", "holds_qual", "highest_level_qualification", "hsc_nvq1", "hsc_nvq2", "hsc_nvq3", "care_nvq1", "care_nvq2", "care_nvq3", "other_nvq", "workplace_sector_vec", "main_service_vec", "main_job_role_vec", "region_vec", "gender_vec", "eea_born_vec", "source_of_recruitment_vec", "contract_type_vec", "holds_scq_vec", "disabled_vec"
]

assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

staff_data = assembler.transform(staff_data)

# COMMAND ----------

staff_data_predictions = rf_best_model.transform(staff_data)

# COMMAND ----------

staff_data_df = staff_data_predictions.select("worker_id", "prediction", "probability").toPandas()

staff_data_df[['probability_class_0', 'probability_class_1']] = pd.DataFrame(staff_data_df['probability'].tolist(), index=staff_data_df.index)

staff_data_df = staff_data_df.drop(columns=['probability'])

staff_data_df = staff_data_df.rename(columns={
    "worker_id": "Worker ID",
    "prediction": "Predicted Status",
    "probability_class_0": "Probability of Staying",
    "probability_class_1": "Probability of Leaving"
})

print(staff_data_df.head(10).to_string(index=False))

# COMMAND ----------

from pyspark.ml.feature import StandardScaler

scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=False)
staff_data_predictions = scaler.fit(staff_data_predictions).transform(staff_data_predictions)

# COMMAND ----------

from sklearn.mixture import GaussianMixture
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

staff_data_pd = staff_data_predictions.select("features").toPandas()
X = np.array(staff_data_pd['features'].tolist())

n_components_range = range(2, 20) 

bic_scores = []
aic_scores = []
n_clusters = []

for n_components in n_components_range:
    gmm = GaussianMixture(n_components=n_components, random_state=288)
    gmm.fit(X)
    
    bic = gmm.bic(X)
    aic = gmm.aic(X)
    
    bic_scores.append(bic)
    aic_scores.append(aic)
    n_clusters.append(n_components)

plt.figure(figsize=(12, 6))
plt.plot(n_clusters, bic_scores, label='BIC')
plt.plot(n_clusters, aic_scores, label='AIC')
plt.xlabel('Number of Clusters')
plt.ylabel('Score')
plt.title('BIC and AIC Scores for GMM')
plt.legend()
plt.show()

best_n_clusters = n_clusters[np.argmin(bic_scores)] 
gmm_best = GaussianMixture(n_components=best_n_clusters, random_state=42)
gmm_labels = gmm_best.fit_predict(X)


# COMMAND ----------

from sklearn.mixture import GaussianMixture
import numpy as np
import pandas as pd

optimal_k = 10 

gmm = GaussianMixture(n_components=optimal_k, random_state=288)
gmm_labels = gmm.fit_predict(X)

staff_data_pd['gmm_cluster'] = gmm_labels

staff_data_clustered = spark.createDataFrame(staff_data_pd)

staff_data_clustered.select("gmm_cluster", "features").show()

# COMMAND ----------

staff_data_clustered.groupBy("gmm_cluster").count().orderBy("gmm_cluster").show()

# COMMAND ----------

from sklearn.decomposition import PCA

pca = PCA(n_components=2)
X_pca = pca.fit_transform(X)

plt.figure(figsize=(10, 8))
plt.scatter(X_pca[:, 0], X_pca[:, 1], c=gmm_labels, cmap='viridis', s=50)
plt.title('GMM Clustering (PCA-reduced)')
plt.xlabel('Principal Component 1')
plt.ylabel('Principal Component 2')
plt.colorbar(label='Cluster Label')
plt.show()

# COMMAND ----------

from sklearn.manifold import TSNE
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

X = np.array(staff_data_clustered.select("features").rdd.map(lambda x: x[0].toArray()).collect())

tsne = TSNE(n_components=2, random_state=288)
X_tsne = tsne.fit_transform(X)

tsne_df = pd.DataFrame(X_tsne, columns=['Component 1', 'Component 2'])
tsne_df['Cluster'] = staff_data_clustered.select("gmm_cluster").toPandas()

plt.figure(figsize=(10, 8))
sns.scatterplot(x='Component 1', y='Component 2', hue='Cluster', palette='viridis', data=tsne_df)
plt.title("t-SNE Clustering")
plt.show()


# COMMAND ----------


