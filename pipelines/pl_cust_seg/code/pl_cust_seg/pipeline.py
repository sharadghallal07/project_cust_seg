from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pl_cust_seg.config.ConfigStore import *
from pl_cust_seg.udfs.UDFs import *
from prophecy.utils import *
from pl_cust_seg.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ds_prod_details = ds_prod_details(spark)
    df_ds_prod_order = ds_prod_order(spark)
    df_rename_product_id_to_prod_id = rename_product_id_to_prod_id(spark, df_ds_prod_order)
    df_by_product_id = by_product_id(spark, df_rename_product_id_to_prod_id, df_ds_prod_details)
    df_ds_category = ds_category(spark)
    df_rename_category_column = rename_category_column(spark, df_ds_category)
    df_by_category_inner_join = by_category_inner_join(spark, df_by_product_id, df_rename_category_column)
    df_ds_customer_data = ds_customer_data(spark)
    df_rename_customer_id_to_cust_id = rename_customer_id_to_cust_id(spark, df_ds_customer_data)
    df_by_customer_id_inner_join = by_customer_id_inner_join(
        spark, 
        df_rename_customer_id_to_cust_id, 
        df_by_category_inner_join
    )
    df_total_purchase_by_customer = total_purchase_by_customer(spark, df_by_customer_id_inner_join)
    df_frequency_by_customer_id = frequency_by_customer_id(spark, df_total_purchase_by_customer)
    df_total_purchase_amount_by_customer = total_purchase_amount_by_customer(spark, df_total_purchase_by_customer)
    ds_total_purchase_by_cust(spark, df_total_purchase_amount_by_customer)
    df_avg_items_per_order = avg_items_per_order(spark, df_total_purchase_by_customer)
    ds_avg_basket_size(spark, df_avg_items_per_order)
    ds_fequency_purchase(spark, df_frequency_by_customer_id)
    df_unique_demographics = unique_demographics(spark, df_total_purchase_by_customer)
    ds_customer_demographics(spark, df_unique_demographics)

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
