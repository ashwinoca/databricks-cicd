from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, year, month, sum as spark_sum


@dp.materialized_view()
def country_aggregate() -> DataFrame:
    """
    Aggregate sales data by country and year-month.
    Calculates total quantity sold and total sales price for each country per month.
    """
    orders = spark.read.table("process_orders")
    
    # Parse invoice date and extract year-month
    # Convert quantity from string to integer and calculate total sale price
    aggregated = (orders
        .withColumn("invoice_date", to_date(col("invoicedate"), "M/d/yyyy H:mm"))
        .withColumn("year", year(col("invoice_date")))
        .withColumn("month", month(col("invoice_date")))
        .withColumn("quantity_int", col("quantity").cast("int"))
        .withColumn("total_sale_price", col("quantity_int") * col("unitprice"))
        .groupBy("country", "year", "month")
        .agg(
            spark_sum("quantity_int").alias("total_quantity"),
            spark_sum("total_sale_price").alias("total_sales")
        )
        .orderBy("country", "year", "month")
    )
    
    return aggregated