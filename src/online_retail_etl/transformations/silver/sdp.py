
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

catalog = spark.conf.get("catalog")


@dp.materialized_view(
      name=f"{catalog}.silver.process_orders"
)
@dp.expect("valid_country", "country!='EIRE'")
@dp.expect_or_drop("valid_country_dropped", "country!='RSA'")
def process_orders() -> DataFrame:
  orders = spark.read.table(f"{catalog}.bronze.raw_orders")
  columns = orders.columns
  for col in columns:
      orders = orders.withColumnRenamed(col, col.lower())

  return orders




