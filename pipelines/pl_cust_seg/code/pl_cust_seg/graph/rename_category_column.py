from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_cust_seg.config.ConfigStore import *
from pl_cust_seg.udfs.UDFs import *

def rename_category_column(spark: SparkSession, in0: DataFrame) -> DataFrame:
    out0 = in0.filter("value is not null").rdd\
               .zipWithIndex()\
               .filter(lambda x: x[1] > 0)\
               .map(lambda x: x[0])\
               .toDF()\
               .withColumn("cat", expr("split(value, ',', 2)[0]"))\
               .withColumn("Description", expr("substring(value, length(split(value, ',', 2)[0])+2)"))\
               .drop("value")

    return out0
