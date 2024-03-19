from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_cust_seg.config.ConfigStore import *
from pl_cust_seg.udfs.UDFs import *

def remove_customer_id(spark: SparkSession, in0: DataFrame) -> DataFrame:
    out0 = in0.drop("CustomerID").groupBy("custID").agg(sum("Price").alias("Total_purchase_amount"))

    return out0
