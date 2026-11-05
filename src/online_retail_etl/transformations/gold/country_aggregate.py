from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, year, month, sum as spark_sum


COUNTRIES = (
    "Australia",
    "Austria",
    "Bahrain",
    "Belgium",
    "Brazil",
    "Canada",
    "Channel Islands",
    "Cyprus",
    "Czech Republic",
    "Denmark",
    "EIRE",
    "European Community",
    "Finland",
    "France",
    "Germany",
    "Greece",
    "Iceland",
    "Israel",
    "Italy",
    "Japan",
    "Lebanon",
    "Lithuania",
    "Malta",
    "Netherlands",
    "Norway",
    "Poland",
    "Portugal",
    "Saudi Arabia",
    "Singapore",
    "Spain",
    "Sweden",
    "Switzerland",
    "United Arab Emirates",
    "United Kingdom",
    "Unspecified",
    "USA",
)


@dp.materialized_view(
    name="retail_pipeline.gold.country_aggregate",
    comment="Aggregated sales data by country and year-month, including total quantity sold and total sales price."
)
def country_aggregate() -> DataFrame:
    """Aggregate total quantity and sales by country and month."""
    orders = spark.read.table("retail_pipeline.silver.process_orders")

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
    )

    return aggregated


for country in COUNTRIES:
    country_slug = country.lower().replace(" ", "_").replace("-", "_")

    @dp.materialized_view(
        name=f"retail_pipeline.gold.country_aggregate_{country_slug}",
        comment=f"Aggregated sales data by month for {country}.",
    )
    def country_aggregate_by_country(country_filter: str = country) -> DataFrame:
        return (spark.read.table("retail_pipeline.gold.country_aggregate")
            .filter(col("country") == country_filter)
        )