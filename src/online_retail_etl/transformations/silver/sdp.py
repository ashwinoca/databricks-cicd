
from pyspark import pipelines as dp
from pyspark.sql import DataFrame


@dp.materialized_view()
def process_orders() -> DataFrame:
  orders = spark.read.table("retail_pipeline.bronze.raw_orders")
  columns = orders.columns
  for col in columns:
      orders = orders.withColumnRenamed(col, col.lower())

  return orders




