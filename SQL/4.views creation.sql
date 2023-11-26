-- Assuming [SalesHubDB].[dbo].[sales_geo_info_final] is the table on which measures are calculated

-- Create a view with additional columns for month, quarter, and year
CREATE VIEW sales_view AS
SELECT
    [order_id],
    [customer_id],
    [product_id],
    [quantity],
    [price],
    [order_date],
	DATEPART(MONTH, [order_date]) AS [month],
    DATEPART(QUARTER, [order_date]) AS [quarter],
    DATEPART(YEAR, [order_date]) AS [year],
    [name],
    [username],
    [email],
    [lat],
    [lng],
    [temperature],
    [weather_description]
    FROM [SalesHubDB].[dbo].[sales_geo_info_final];

-- Create a view for total sales per customer
CREATE VIEW total_sales_per_customer_view AS
SELECT
    [customer_id],
    SUM([quantity] * [price]) AS total_sales
FROM sales_view
GROUP BY [customer_id];

-- Create a view for average order quantity per product
CREATE VIEW average_order_quantity_per_product_view AS
SELECT
    [product_id],
    AVG([quantity]) AS average_order_quantity
FROM sales_view
GROUP BY [product_id];

-- Create a view for top-selling products
CREATE VIEW top_selling_products_view AS
SELECT TOP (100) PERCENT
    [product_id],
    SUM([quantity] * [price]) AS total_sales
FROM sales_view
GROUP BY [product_id]
ORDER BY total_sales DESC;

-- Create a view for top customers
CREATE VIEW top_customers_view AS
SELECT TOP (100) PERCENT
    [customer_id],
    SUM([quantity] * [price]) AS total_sales
FROM sales_view
GROUP BY [customer_id]
ORDER BY total_sales DESC;

-- Create a view for monthly sales trends
CREATE VIEW monthly_sales_trends_view AS
SELECT
    DATEPART(MONTH, [order_date]) AS [month],
    SUM([quantity] * [price]) AS total_sales
FROM sales_view
GROUP BY DATEPART(MONTH, [order_date]);

-- Create a view for average sales per weather condition
CREATE VIEW average_sales_per_weather_view AS
SELECT
    [weather_description],
    AVG([quantity] * [price]) AS average_sales
FROM sales_view
GROUP BY [weather_description];
