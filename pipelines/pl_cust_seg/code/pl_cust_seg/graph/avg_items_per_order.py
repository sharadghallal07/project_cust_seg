from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_cust_seg.config.ConfigStore import *
from pl_cust_seg.udfs.UDFs import *

def avg_items_per_order(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import avg
    out0 = in0.groupBy("OrderID").agg(avg("Quantity").alias("avg_items_per_order"))

    return out0
