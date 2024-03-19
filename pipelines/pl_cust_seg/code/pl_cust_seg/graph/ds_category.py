from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_cust_seg.config.ConfigStore import *
from pl_cust_seg.udfs.UDFs import *

def ds_category(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("text")\
        .schema(StructType([StructField("value", StringType(), True)]))\
        .text("dbfs:/FileStore/POC01/Category_Description.txt", wholetext = False, lineSep = None)
