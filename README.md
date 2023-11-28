Data Pipeline Documentation

Step 1: Data Acquisition and Preprocessing

Components
1. Data Acquisition and Preprocessing

1.1 Import Necessary Libraries
This component involves importing essential Python libraries, such as requests for API calls, pandas for data manipulation, and matplotlib and seaborn for visualization.


import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


1.2 Read Sales Data from CSV File
In this step, the pipeline reads sales data from a CSV file named "sales_data.csv."

sales_data = pd.read_csv("sales_data.csv")

Data Cleansing: Remove Duplicates
Duplicates in the sales data are removed based on the 'order_id,' keeping the row with the minimum 'order_date' so that later entry of duplicates will be removed

sales_data = sales_data.sort_values('order_date').drop_duplicates('order_id', keep='first')

Save Cleansed Sales Data to a New CSV File
The cleaned sales data is saved to a new CSV file named "sales_data_final.csv", to ensure and verify the duplicates are  removed 

sales_data.to_csv('sales_data_final.csv', index=False)
----------------------------------------------------------------------------------------------------------------------------------------------------

Step 2: Data Transformation from JSONPlaceholder API

2.1 Fetch User Data from JSONPlaceholder API
User data is obtained from the JSONPlaceholder API using the requests library.

response = requests.get('https://jsonplaceholder.typicode.com/users')
user_data = response.json()

2.2 Extract Relevant Fields and Convert to DataFrame
The obtained user data is processed to extract relevant fields and converted into a DataFrame. The fields include 'customer_id,' 'name,' 'username,' 'email,' 'lat,' and 'lng.'

Create a DataFrame with relevant user information, including 'customer_id', 'name', 'username', 'email', 'lat', and 'lng':

user_df = pd.DataFrame([
    {**user, 'customer_id': user['id'], 'lat': user['address']['geo']['lat'], 'lng': user['address']['geo']['lng']}
    for user in user_data
], columns=['customer_id', 'name', 'username', 'email', 'lat', 'lng'])

2.3 Merge User Data with Sales Data based on Customer_ID
The user data is merged with the sales data based on the 'customer_id.' This step ensures that each customer's information is associated with their corresponding sales data.


Check for missing values in 'customer_id' and convert data types if necessary before merging:


if sales_data['customer_id'].isna().any() or user_df['customer_id'].isna().any():
    print("There are missing values in 'customer_id'. Please handle them before merging.")

# Handle missing values in 'customer_id'
user_df = user_df.dropna(subset=['customer_id'])

# Convert 'customer_id' to int if necessary
# (Ensure data types match before merging)

if sales_data['customer_id'].dtype != user_df['customer_id'].dtype:
    if sales_data['customer_id'].dtype == object:
        sales_data['customer_id'] = sales_data['customer_id'].astype(int)
    elif user_df['customer_id'].dtype == object:
        user_df['customer_id'] = user_df['customer_id'].astype(int)

# Merge data after removing duplicates in 'order_id'
merged_data = sales_data.merge(user_df, on='customer_id')

# Save the merged data to a CSV file
merged_data.to_csv('merged_data.csv', index=False)


Step 3: Data Transformation from OpenWeatherMap API

3.1 Create a Function to Fetch Weather Data

def get_weather_data(lat, lng):
    api_key = '4f43cd2e846d0b7f3047fc12e268c182'
    url =  f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lng}&appid={api_key}'
    response = requests.get(url)
    weather_data = response.json()
    return weather_data

3.2 Fetch Weather Data for Each Sale

for index, row in merged_data.iterrows():
    if 'lat' in row.index and 'lng' in row.index:
        latitude = row['lat']
        longitude = row['lng']

        try:
            # Fetch weather data using latitude and longitude
            weather_data = get_weather_data(latitude, longitude)

            # Extract relevant weather information
            temperature = weather_data['main']['temp']
            description = weather_data['weather'][0]['description']

            # Add weather information to sales data
            merged_data.loc[index, 'temperature'] = temperature
            merged_data.loc[index, 'weather_description'] = description
        except Exception as e:
            print(f"Weather data unavailable for coordinates: {latitude},{longitude}")
            print(e)
    else:
        print(f"Skipping weather data for row: {index}")

# Save updated sales data with weather information
merged_data.to_csv('merged_sales_data_with_weather.csv', index=False)

---------------------------------------------------------------------------------------------------------------------------------------------------

3. Analysis

3.1 Load Merged Dataset with Weather Information

The merged dataset, which includes weather information, is loaded for further analysis and visualization.

merged_data = pd.read_csv('merged_sales_data_with_weather.csv')

merged_data['order_date'] = pd.to_datetime(merged_data['order_date'])

3.2 Calculate Total Sales per Customer

Total sales amount per customer is calculated by multiplying the 'quantity' and 'price' columns.

merged_data['total_sales'] = merged_data['quantity'] * merged_data['price']

total_sales_per_customer = merged_data.groupby('customer_id')['total_sales'].sum().reset_index()

3.3 Determine Average Order Quantity per Product

The average order quantity per product is determined by grouping the data by 'product_id' and calculating the mean of the 'quantity' column.

average_order_quantity_per_product = merged_data.groupby('product_id')['quantity'].mean().reset_index()

3.4 Identify Top-Selling Products

Top-selling products are identified based on the total sales amount. The data is sorted in descending order.

top_selling_products = merged_data.groupby('product_id')['total_sales'].sum().sort_values(ascending=False).reset_index()

3.5 Identify Top Customers

Top customers are identified based on their total purchase amount. The data is sorted in descending order.

top_customers = merged_data.groupby('customer_id')['total_sales'].sum().sort_values(ascending=False).reset_index()

3.6 Analyze Sales Trends Over Time

Monthly sales trends are analyzed by grouping the data by month and summing the total sales amount.

monthly_sales_trends = merged_data.groupby(merged_data['order_date'].dt.to_period("M"))['total_sales'].sum().reset_index()

3.7 Analyze Average Sales per Weather Condition

Average sales amount per weather condition is analyzed by grouping the data by 'weather_description' and calculating the mean of the total sales amount.

average_sales_per_weather = merged_data.groupby('weather_description')['total_sales'].mean().reset_index()

3.8 Save Results to CSV (or Other Formats)

The calculated results are saved to CSV files for future analysis or reporting.


total_sales_per_customer.to_csv('total_sales_per_customer.csv', index=False)
average_order_quantity_per_product.to_csv('average_order_quantity_per_product.csv', index=False)
top_selling_products.to_csv('top_selling_products.csv', index=False)
top_customers.to_csv('top_customers.csv', index=False)
monthly_sales_trends.to_csv('monthly_sales_trends.csv', index=False)
average_sales_per_weather.to_csv('average_sales_per_weather.csv', index=False)
--------------------------------------------------------------------------------------------------------------------------------------------------------

4. Visualization
4.1 Set Seaborn Style
The Seaborn style is set to "whitegrid" for consistent and visually appealing plots.

sns.set(style="whitegrid")

4.2 Visualize Total Sales per Customer
A bar plot is generated to visualize the total sales amount per customer.

plt.figure(figsize=(12, 6))
sns.barplot(x='customer_id', y='total_sales', data=total_sales_per_customer, palette='viridis')
plt.title('Total Sales per Customer')
plt.xlabel('Customer ID')
plt.ylabel('Total Sales')
plt.show()

4.3 Visualize Average Order Quantity per Product
A bar plot is generated to visualize the average order quantity per product.

plt.figure(figsize=(12, 6))
sns.barplot(x='product_id', y='quantity', data=average_order_quantity_per_product, palette='plasma')
plt.title('Average Order Quantity per Product')
plt.xlabel('Product ID')
plt.ylabel('Average Quantity')
plt.show()

4.4 Visualize Top Selling Products
A bar plot is generated to visualize the top-selling products based on total sales amount.

plt.figure(figsize=(12, 6))
sns.barplot(x='product_id', y='total_sales', data=top_selling_products.head(10), palette='mako')
plt.title('Top Selling Products')
plt.xlabel('Product ID')
plt.ylabel('Total Sales')
plt.show()

4.5 Visualize Top Customers
A bar plot is generated to visualize the top customers based on their total purchase amount.

plt.figure(figsize=(12, 6))
sns.barplot(x='customer_id', y='total_sales', data=top_customers.head(10), palette='rocket')
plt.title('Top Customers')
plt.xlabel('Customer ID')
plt.ylabel('Total Sales')
plt.show()

4.6 Visualize Monthly Sales Trends
A line plot is generated to visualize the monthly sales trends over time.

plt.figure(figsize=(12, 6))
sns.lineplot(x='order_date', y='total_sales', data=monthly_sales_trends, marker='o', palette='coolwarm')
plt.title('Monthly Sales Trends')
plt.xlabel('Order Date')
plt.ylabel('Total Sales')
plt.xticks(rotation=45)
plt.show()

4.7 Visualize Average Sales per Weather Condition
A bar plot is generated to visualize the average sales amount per weather condition.

plt.figure(figsize=(12, 6))
sns.barplot(x='weather_description', y='total_sales', data=average_sales_per_weather, palette='pastel')
plt.title('Average Sales per Weather Condition')
plt.xlabel('Weather Description')
plt.ylabel('Average Sales')
plt.xticks(rotation=45, ha='right')
plt.show()


Additional Information
Assumptions:

Assumes all the files 'sales_data.csv', 'merged_data.csv' and 'merged_sales_data_with_weather.csv' are present in the same directory as the script.
Assumes that there are no missing values in 'customer_id' in both sales and user data( id column renamed to customer_id for merging ).
Data types of 'customer_id' are ensured to match before merging.

Running the Pipeline:

Ensure all necessary libraries are installed (requests, pandas, matplotlib, seaborn).
Copy and paste each section of the code into a Python script or Jupyter notebook.
Run each section sequentially.

Notes:

Results are saved in CSV files for further analysis or sharing.
Visualizations are generated using Seaborn and Matplotlib for insights into sales trends, top customers, and product performance.


Dependencies
Python Libraries:
requests: Used for making API requests.
pandas: Essential for data manipulation and analysis.
matplotlib and seaborn: Used for data visualization.

External Data Sources:

CSV files: The pipeline relies on existing CSV files, specifically "sales_data.csv" and "merged_sales_data_with_weather.csv."

API Dependency:
The pipeline fetches user data from the JSONPlaceholder API. Ensure a stable internet connection for API calls.
--------------------------------------------------------------------------------------------------------------------------------

SSMS Database Schema and Tables Description

Tables are loaded sequentially

1.Table [dbo].[sales_data] -- Table loaded from sales_data_final.csv (sales_data.csv file with removed duplicates)


USE [SalesHubDB]
GO

/****** Object:  Table [dbo].[sales_data]   ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[sales_data](
	[order_id] [int] NOT NULL,
	[customer_id] [int] NULL,
	[product_id] [int] NULL,
	[quantity] [int] NULL,
	[price] [decimal](10, 2) NULL,
	[order_date] [date] NULL,
PRIMARY KEY CLUSTERED 
(
	[order_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


Data Load Query:

-- Use BULK INSERT to load data from CSV sales_data_final.csv into the sales_data table

BULK INSERT sales_data
FROM 'C:\Users\kvlns\OneDrive\Desktop\codingchallengecloudsourcedataengineerrole\sales_data_final.csv'  
WITH (
    FIELDTERMINATOR = ',',  
    ROWTERMINATOR = '\n',  
    FIRSTROW = 2,           
    CODEPAGE = 'UTF-8'        
);
------------------------------------------------------------------------------------------------------------------------------------------------------

2.Table [dbo].[sales_geo_info]-- Table loaded from merged_data.csv (lat and long)


USE [SalesHubDB]
GO

/****** Object:  Table [dbo].[sales_geo_info]  ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[sales_geo_info](
	[order_id] [int] NOT NULL,
	[customer_id] [int] NULL,
	[product_id] [int] NULL,
	[quantity] [int] NULL,
	[price] [decimal](10, 2) NULL,
	[order_date] [datetime] NULL,
	[name] [nvarchar](255) NULL,
	[username] [nvarchar](255) NULL,
	[email] [nvarchar](255) NULL,
	[lat] [float] NULL,
	[lng] [float] NULL,
PRIMARY KEY CLUSTERED 
(
	[order_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[sales_geo_info]  WITH CHECK ADD  CONSTRAINT [fk_sales_data_geo_info] FOREIGN KEY([order_id])
REFERENCES [dbo].[sales_data] ([order_id])
GO

ALTER TABLE [dbo].[sales_geo_info] CHECK CONSTRAINT [fk_sales_data_geo_info]
GO


Data Load Query:

-- Use BULK INSERT to load data from CSV merged_data.csv into the sales_geo_info table 
BULK INSERT sales_geo_info
FROM 'C:\Users\kvlns\OneDrive\Desktop\codingchallengecloudsourcedataengineerrole\merged_data.csv'  
WITH (
    FIELDTERMINATOR = ',',  
    ROWTERMINATOR = '\n',  
    FIRSTROW = 2,           
    CODEPAGE = 'UTF-8'        
);
--------------------------------------------------------------------------------------------------------------------------------------------------------

3.Table [dbo].[sales_geo_info_final]-- Table loaded from merged_sales_data_with_weather.csv

USE [SalesHubDB]
GO

/****** Object:  Table [dbo].[sales_geo_info_final] ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[sales_geo_info_final](
	[order_id] [int] NOT NULL,
	[customer_id] [int] NULL,
	[product_id] [int] NULL,
	[quantity] [int] NULL,
	[price] [decimal](10, 2) NULL,
	[order_date] [datetime] NULL,
	[name] [nvarchar](255) NULL,
	[username] [nvarchar](255) NULL,
	[email] [nvarchar](255) NULL,
	[lat] [float] NULL,
	[lng] [float] NULL,
	[temperature] [float] NULL,
	[weather_description] [nvarchar](255) NULL,
PRIMARY KEY CLUSTERED 
(
	[order_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[sales_geo_info_final]  WITH CHECK ADD  CONSTRAINT [fk_sales_data_final] FOREIGN KEY([order_id])
REFERENCES [dbo].[sales_data] ([order_id])
GO

ALTER TABLE [dbo].[sales_geo_info_final] CHECK CONSTRAINT [fk_sales_data_final]
GO

ALTER TABLE [dbo].[sales_geo_info_final]  WITH CHECK ADD  CONSTRAINT [fk_sales_geo_info_final] FOREIGN KEY([order_id])
REFERENCES [dbo].[sales_geo_info] ([order_id])
GO

ALTER TABLE [dbo].[sales_geo_info_final] CHECK CONSTRAINT [fk_sales_geo_info_final]
GO


-- Use BULK INSERT to load data from CSV merged_sales_data_with_weather.csv into the sales_geo_info_final table (temp and weather desciption) 

BULK INSERT sales_geo_info_final
FROM 'C:\Users\kvlns\OneDrive\Desktop\codingchallengecloudsourcedataengineerrole\merged_sales_data_with_weather.csv'  
WITH (
    FIELDTERMINATOR = ',',  
    ROWTERMINATOR = '\n',  
    FIRSTROW = 2,           
    CODEPAGE = 'UTF-8'        
);
------------------------------------------------------------------------------------------------------------------

Data Acquisition and Preprocessing:

Remove Duplicates based on 'order_id':

-- Assuming 'sales_data' table has columns order_id,customer_id,product_id,quantity,price,order_date columns

WITH RankedSalesData AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date) AS RowNum
    FROM
        sales_data
)

DELETE FROM RankedSalesData WHERE RowNum > 1;

Save Cleansed Sales Data to a New Table:

-- Assuming 'sales_data_final' file has the same schema as 'sales_data'

CREATE TABLE sales_data AS
SELECT * FROM RankedSalesData;
----------------------------------------------------------------------------------------------------------------
Analysis and Aggregation:

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
