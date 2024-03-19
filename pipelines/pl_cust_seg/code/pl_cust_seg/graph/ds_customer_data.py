from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_cust_seg.config.ConfigStore import *
from pl_cust_seg.udfs.UDFs import *

def ds_customer_data(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("CustomerID", StringType(), True), StructField("Age", StringType(), True), StructField("Gender", StringType(), True), StructField("Location", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/POC01/Customer_Data.txt")
