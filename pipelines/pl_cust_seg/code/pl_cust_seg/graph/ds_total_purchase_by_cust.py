from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_cust_seg.config.ConfigStore import *
from pl_cust_seg.udfs.UDFs import *

def ds_total_purchase_by_cust(spark: SparkSession, in0: DataFrame):
    in0.write\
        .option("header", True)\
        .option("sep", ",")\
        .mode("error")\
        .option("separator", ",")\
        .option("header", True)\
        .csv("dbfs:/FileStore/POC01/Output/Purchase_Order_By_Cust")
