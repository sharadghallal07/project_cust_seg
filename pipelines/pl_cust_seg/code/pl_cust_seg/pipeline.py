from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pl_cust_seg.config.ConfigStore import *
from pl_cust_seg.udfs.UDFs import *
from prophecy.utils import *
from pl_cust_seg.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ds_prod_order = ds_prod_order(spark)
    df_ds_prod_details = ds_prod_details(spark)
    df_by_product_id = by_product_id(spark, df_ds_prod_order, df_ds_prod_details)
    df_ds_customer_data = ds_customer_data(spark)
    df_rename_customer_id_to_custID = rename_customer_id_to_custID(spark, df_ds_customer_data)
    df_by_customer_id_inner_join = by_customer_id_inner_join(spark, df_rename_customer_id_to_custID, df_ds_prod_order)
    df_remove_customer_id = remove_customer_id(spark, df_by_customer_id_inner_join)
    df_ds_category = ds_category(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pl_cust_seg")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/pl_cust_seg", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/pl_cust_seg")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
