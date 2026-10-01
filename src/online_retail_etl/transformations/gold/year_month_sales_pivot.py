from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, lpad


@dp.materialized_view(
    name="year_month_sales_pivot",
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
        .groupBy("country")
        .pivot("year_month")
        .sum("total_sales")
        .orderBy("year_month")
    )
    
    return pivoted