from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_cust_seg.config.ConfigStore import *
from pl_cust_seg.udfs.UDFs import *

def by_product_id(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0.alias("in0").join(in1.alias("in1"), (col("in0.ProductID") == col("in1.ProductID")), "inner")
