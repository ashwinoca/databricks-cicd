from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, lpad


@dp.materialized_view(
    table_properties={
        "delta.columnMapping.mode": "name"
    }
)
def country_sales_pivot() -> DataFrame:
    """
    Dynamic pivot view showing total sales by country (columns) and year-month (rows).
    Each country becomes a column with sales values.
    """
    agg_data = spark.read.table("country_aggregate")
    
    # Create year_month column for better readability (format: YYYY-MM)
    pivoted = (agg_data
        .withColumn("year_month", 
            concat_ws("-", 
                col("year").cast("string"), 
                lpad(col("month").cast("string"), 2, "0")))
        .groupBy("year_month")
        .pivot("country")
        .sum("total_sales")
        .orderBy("year_month")
    )
    
    return pivoted