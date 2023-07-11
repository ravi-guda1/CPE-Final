from pyspark.sql import SparkSession

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Products Table") \
    .enableHiveSupport() \
    .getOrCreate()

# Define the schema for the Products table
schema = StructType([
    StructField("ProductID", IntegerType(), nullable=False),
    StructField("ProductName", StringType(), nullable=False),
    StructField("CategoryID", IntegerType(), nullable=False),
    StructField("SupplierID", IntegerType(), nullable=False),
    StructField("UnitPrice", DoubleType(), nullable=False),
    StructField("UnitsInStock", IntegerType(), nullable=False)
])

# Create the Products DataFrame with the defined schema
products_data = [
    (1, "Product A", 1, 1, 100.0, 10),
    (2, "Product B", 2, 2, 200.0, 5),
    # Add more product records...
]

products_df = spark.createDataFrame(products_data, schema)
#products_df.saveAsTable("products_df")
#df.write.saveAsTable("database_name.table_name")

# Create or replace a temporary view for the Products table
products_df.createOrReplaceTempView("products")
spark.sql("create table as SELECT * FROM products").show()

spark.read.table("products")

# Perform operations on the Products table
# Example: Show all products
spark.sql("SELECT * FROM products").show()

# Stop the SparkSession
#spark.stop()
