from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_cust_seg.config.ConfigStore import *
from pl_cust_seg.udfs.UDFs import *

def ds_prod_order(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("OrderID", StringType(), True), StructField("CustomerID", StringType(), True), StructField("ProductID", StringType(), True), StructField("Quantity", StringType(), True), StructField("Price", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/POC01/Product_Order_Data.txt")
