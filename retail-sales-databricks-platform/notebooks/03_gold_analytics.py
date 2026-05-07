from pyspark.sql.functions import month
from pyspark.sql.functions import sum, round

gold_df = spark.table("workspace.default.sales_silver")
display(gold_df)


# City Sales Aggregation
# save gold table: sales_by_city

sales_by_city = gold_df.groupBy("city").agg(
    round(sum("sales"), 2).alias("total_sales")
)

display(sales_by_city)

sales_by_city.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.sales_by_city")

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


from pyspark.sql.functions import col, sum, round

# save gold table: top_products
top_products = gold_df.groupBy("product_line").agg(
    round(sum("sales"), 2).alias("total_sales")
).orderBy(col("total_sales").desc())

display(top_products)

top_products.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.top_products")