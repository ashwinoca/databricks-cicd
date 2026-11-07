CREATE OR REFRESH STREAMING TABLE ${catalog}.bronze.raw_orders
AS
SELECT
    InvoiceNo,
    StockCode,
    Description,
    Quantity,
    InvoiceDate,
    UnitPrice,
    CustomerID,
    Country,
    current_timestamp() as ingestion_timestamp,
    _metadata.file_path as source_file
FROM STREAM read_files(
    '/Volumes/${catalog}/bronze/source_files',
    format => 'csv',
    header => true,
    schema => 'InvoiceNo STRING, StockCode STRING, Description STRING, Quantity STRING, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID STRING, Country STRING'
);
