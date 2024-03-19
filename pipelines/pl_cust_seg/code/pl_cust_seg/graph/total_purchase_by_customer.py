from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_cust_seg.config.ConfigStore import *
from pl_cust_seg.udfs.UDFs import *

def total_purchase_by_customer(spark: SparkSession, in0: DataFrame) -> DataFrame:
    out0 = in0\
               .drop("CustID", "ProdID", "cat", "Description")\
               .withColumn("CustomerID", col("CustomerID").cast("integer"))\
               .withColumn("Age", col("Age").cast("integer"))\
               .withColumn("OrderID", col("OrderID").cast("integer"))\
               .withColumn("Quantity", col("Quantity").cast("integer"))\
               .withColumn("Price", col("Price").cast("integer"))

    return out0
