from yaml_remove import yaml_remove
from pyspark.sql import SparkSession
from Extract import extract
from pyspark.sql import functions as F
from to_mysql import to_mysql

# Start Spark Session
spark = SparkSession.builder.appName("PySpark MySQL connection example").config(
    "spark.jars", "mysql-connector-j-9.0.0.jar").getOrCreate()

# Extract data
table = extract(spark)
customer_data = table[0]
order_data = table[1]
discount_data = table[2]

# Join Customer and Order Table
df = customer_data.join(order_data, customer_data["ID"] ==
                        order_data["Customer ID"], how="inner")
# Filter only discount data more than 10 Sep
discount_data = discount_data.filter(
    F.col("Start_Date").cast("timestamp") > F.lit('2024-09-10').cast("timestamp"))

# Join Merge Table with discount Table
df = df.join(discount_data, df["Product_ID"] ==
             discount_data["Product_ID"], how="left")

# Calculate Total Spend on each order
df = df.withColumn("Total",
                   F.when(
                       F.col("Discount").isNull(),
                       F.col("Price") * F.col("purchase_unit")
                   ).otherwise(
                       F.col("Price") * F.col("purchase_unit") * ((100 - F.col("Discount"))/100))
                   )

# Group By
df_2 = df.groupBy("Customer_Name").agg(F.sum("Total"))

# Rename
df_2 = df_2.withColumnRenamed("sum(Total)", "Total Spent")
df_2 = df_2.withColumnRenamed("Customer_Name", "Customer Name")

df.show()
print(df_2.show())

# Add to mysql
to_mysql(spark, df_2)

# Remove Password
yaml_remove()

# End Spark Session
spark.stop()
