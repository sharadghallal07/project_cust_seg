from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_cust_seg.config.ConfigStore import *
from pl_cust_seg.udfs.UDFs import *

def total_purchase_amount_by_customer(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import sum
    out0 = in0.groupBy("CustomerID").agg(sum("Price").alias("TotalPurchaseAmount"))

    return out0
