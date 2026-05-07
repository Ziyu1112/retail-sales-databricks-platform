from pyspark.sql.functions import *

# Read raw table
df = spark.table("workspace.default.super_market_analysis")

# Preview data
display(df)

df.printSchema()

