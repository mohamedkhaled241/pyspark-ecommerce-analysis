from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import to_date, col, when, sum, row_number, count

spark = SparkSession.builder \
    .appName("Ecommerce Analysis") \
    .getOrCreate()

# ================= USERS =================

df_users = spark.read.csv("data/users.csv",header= True , inferSchema= True)

df_users = df_users.withColumn("signup_date", to_date("signup_date"))

# ================= ORDERS =================
df_orders = spark.read.csv("data/orders.csv", header= True , inferSchema= True)

# ================= CLEANING =================
df_orders = df_orders.withColumn("is_product_missing",when(col("product").isNull(),1).otherwise(0))
df_orders = df_orders.withColumn("is_price_missing",when(col("price").isNull(),1).otherwise(0))
df_orders_clean = df_orders.filter(
    (col("is_price_missing") == 0 ) &
     (col("is_product_missing") == 0)
)

# ================= JOIN =================
df_join = df_users.join(df_orders_clean, on="user_id", how="inner")

df_final = df_join.select("user_id","name","city","product","price")

# ================= TOTAL SPENT =================
df_total = df_join.groupBy("user_id","name","city") \
    .agg(sum("price").alias("total_spent"))

# ================= SEGMENTATION =================
df_total = df_total.withColumn(
    "customer_status",
    when(col("total_spent") > 15000, "VIP").otherwise("Normal")
)

# ================= TOP USERS =================
window_user = Window.partitionBy("city").orderBy(col("total_spent").desc())

df_top_users = df_total.withColumn(
    "rank",
    row_number().over(window_user)
).filter(col("rank") <= 3)

# ================= TOP PRODUCT =================
df_product = df_final.groupBy("city","product") \
    .agg(count("product").alias("order_count"))

window_product = Window.partitionBy("city").orderBy(col("order_count").desc())

df_top_product = df_product.withColumn(
    "rank",
    row_number().over(window_product)
).filter(col("rank") <= 3)

# ================= VIP COUNT =================
df_vip = df_total.groupBy("city").agg(
    sum(
        when(col("customer_status") == "VIP", 1).otherwise(0)
    ).alias("vip_count")
)

# ================= OUTPUT =================
print("Top Users per City")
df_top_users.show()

print("Top Product per City")
df_top_product.show()

print("VIP Count per City")
df_vip.show()

# ================= WRITE =================
df_top_users.toPandas().to_csv("output/top_users.csv", index=False)
df_top_product.toPandas().to_csv("output/top_product.csv", index=False)
df_vip.toPandas().to_csv("output/vip_count.csv", index=False)

"""
df_top_users.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/top_users")

df_top_product.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/top_product")

df_vip.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/vip_count")

"""