# Databricks notebook source
# MAGIC %md
# MAGIC Data injection

# COMMAND ----------

from pyspark.sql.functions import *

# Read raw table
df = spark.table("workspace.default.super_market_analysis")

# Preview data
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Brown to Silver 

# COMMAND ----------

# Standardize column names: add underscore to column names

clean_columns = [
    c.lower().replace(" ", "_")
    for c in df.columns
]

df = df.toDF(*clean_columns)

display(df)

# COMMAND ----------

# check the amount of null value
null_counts = df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
])

display(null_counts)

# COMMAND ----------

# check duplicate number
# Count total rows
total_rows = df.count()

# Count distinct rows
distinct_rows = df.distinct().count()

print(f"Total Rows: {total_rows}")
print(f"Distinct Rows: {distinct_rows}")
print(f"Duplicate Rows: {total_rows - distinct_rows}")

# COMMAND ----------

# Check invalid sales values

invalid_sales = df.filter(col("sales") < 0)
display(invalid_sales)

# COMMAND ----------

# save silver table

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.sales_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC Silver to Gold

# COMMAND ----------

gold_df = spark.table("workspace.default.sales_silver")

display(gold_df)

# COMMAND ----------

# City Sales Aggregation
# save gold table: sales_by_city

from pyspark.sql.functions import sum, round

sales_by_city = gold_df.groupBy("city").agg(
    round(sum("sales"), 2).alias("total_sales")
)

display(sales_by_city)

sales_by_city.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.sales_by_city")

# COMMAND ----------

from pyspark.sql.functions import month

# save gold table: monthly_revenue
monthly_revenue = gold_df.groupBy(
    month("date").alias("month")
).agg(
    round(sum("sales"), 2).alias("monthly_sales")
)

display(monthly_revenue)


monthly_revenue.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.monthly_revenue")

# COMMAND ----------

from pyspark.sql.functions import col, sum, round

# save gold table: top_products
top_products = gold_df.groupBy("product_line").agg(
    round(sum("sales"), 2).alias("total_sales")
).orderBy(col("total_sales").desc())

display(top_products)

top_products.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.top_products")