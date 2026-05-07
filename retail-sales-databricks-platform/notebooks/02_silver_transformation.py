# Standardize column names: add underscore to column names

clean_columns = [
    c.lower().replace(" ", "_")
    for c in df.columns
]

df = df.toDF(*clean_columns)

display(df)

# check the amount of null value
null_counts = df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
])

display(null_counts)


# check duplicate number
total_rows = df.count()
distinct_rows = df.distinct().count()

print(f"Total Rows: {total_rows}")
print(f"Distinct Rows: {distinct_rows}")
print(f"Duplicate Rows: {total_rows - distinct_rows}")


# Check invalid sales values
invalid_sales = df.filter(col("sales") < 0)
display(invalid_sales)


# save silver table
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.sales_silver")