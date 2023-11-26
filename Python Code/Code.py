# Step 1: Data Acquisition and Preprocessing

'''1. Import necessary libraries:''' 
import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

'''2. Read sales data from CSV file:''' 
sales_data = pd.read_csv("sales_data.csv")

# Remove duplicates from sales_data based on 'order_id' and keep the row with the minimum 'order_date'
sales_data = sales_data.sort_values('order_date').drop_duplicates('order_id', keep='first')

# Save the cleansed sales_data to a CSV file
sales_data.to_csv('sales_data_final.csv', index=False)

# Step 2: Data Transformation from JSONPlaceholder API

'''1. Fetch user data from JSONPlaceholder API:'''
response = requests.get('https://jsonplaceholder.typicode.com/users')
user_data = response.json()

'''2. Extract relevant fields and convert to DataFrame:'''
user_df = pd.DataFrame([
    {**user, 'customer_id': user['id'], 'lat': user['address']['geo']['lat'], 'lng': user['address']['geo']['lng']}
    for user in user_data
], columns=['customer_id', 'name', 'username', 'email', 'lat', 'lng'])

# print(user_df[:3])

'''3. Merge user data with sales data based on customer_id:'''
if sales_data['customer_id'].isna().any() or user_df['customer_id'].isna().any():
    print("There are missing values in 'customer_id'. Please handle them before merging.")



# Convert 'customer_id' to int if necessary
if sales_data['customer_id'].dtype != user_df['customer_id'].dtype:
    if sales_data['customer_id'].dtype == object:
        sales_data['customer_id'] = sales_data['customer_id'].astype(int)
    elif user_df['customer_id'].dtype == object:
        user_df['customer_id'] = user_df['customer_id'].astype(int)

# Merge data after removing duplicates in 'order_id'
merged_data = sales_data.merge(user_df, on='customer_id')

# Save the merged data to a CSV file
merged_data.to_csv('merged_data.csv', index=False)

#---------------------------------------------------------------------------------------------------------------------------

# Load the merged dataset with weather information
merged_data = pd.read_csv('merged_sales_data_with_weather.csv')

# Convert 'order_date' to datetime format
merged_data['order_date'] = pd.to_datetime(merged_data['order_date'])

# Calculate total sales amount per customer
merged_data['total_sales'] = merged_data['quantity'] * merged_data['price']
total_sales_per_customer = merged_data.groupby('customer_id')['total_sales'].sum().reset_index()

# Determine the average order quantity per product
average_order_quantity_per_product = merged_data.groupby('product_id')['quantity'].mean().reset_index()

# Identify the top-selling products
top_selling_products = merged_data.groupby('product_id')['total_sales'].sum().sort_values(ascending=False).reset_index()

# Identify the top customers
top_customers = merged_data.groupby('customer_id')['total_sales'].sum().sort_values(ascending=False).reset_index()

# Analyze sales trends over time (e.g., monthly or quarterly sales)
monthly_sales_trends = merged_data.groupby(merged_data['order_date'].dt.to_period("M"))['total_sales'].sum().reset_index()

# Analyze average sales amount per weather condition
average_sales_per_weather = merged_data.groupby('weather_description')['total_sales'].mean().reset_index()

# Print or save the results as needed
print("Total Sales per Customer:")
print(total_sales_per_customer)

print("\nAverage Order Quantity per Product:")
print(average_order_quantity_per_product)

print("\nTop Selling Products:")
print(top_selling_products.head())

print("\nTop Customers:")
print(top_customers.head())

print("\nMonthly Sales Trends:")
print(monthly_sales_trends)

print("\nAverage Sales per Weather Condition:")
print(average_sales_per_weather)

# Save results to CSV or other formats if needed
total_sales_per_customer.to_csv('total_sales_per_customer.csv', index=False)
average_order_quantity_per_product.to_csv('average_order_quantity_per_product.csv', index=False)
top_selling_products.to_csv('top_selling_products.csv', index=False)
top_customers.to_csv('top_customers.csv', index=False)
monthly_sales_trends.to_csv('monthly_sales_trends.csv', index=False)
average_sales_per_weather.to_csv('average_sales_per_weather.csv', index=False)

#------------------------------------------------------------------------------------------------------------------------



# Load the aggregated data
total_sales_per_customer = pd.read_csv('total_sales_per_customer.csv')
average_order_quantity_per_product = pd.read_csv('average_order_quantity_per_product.csv')
top_selling_products = pd.read_csv('top_selling_products.csv')
top_customers = pd.read_csv('top_customers.csv')
monthly_sales_trends = pd.read_csv('monthly_sales_trends.csv')
average_sales_per_weather = pd.read_csv('average_sales_per_weather.csv')

# Set the style for seaborn
sns.set(style="whitegrid")

# Visualize Total Sales per Customer
plt.figure(figsize=(12, 6))
sns.barplot(x='customer_id', y='total_sales', data=total_sales_per_customer, palette='viridis')
plt.title('Total Sales per Customer')
plt.xlabel('Customer ID')
plt.ylabel('Total Sales')
plt.show()

# Visualize Average Order Quantity per Product
plt.figure(figsize=(12, 6))
sns.barplot(x='product_id', y='quantity', data=average_order_quantity_per_product, palette='plasma')
plt.title('Average Order Quantity per Product')
plt.xlabel('Product ID')
plt.ylabel('Average Quantity')
plt.show()

# Visualize Top Selling Products
plt.figure(figsize=(12, 6))
sns.barplot(x='product_id', y='total_sales', data=top_selling_products.head(10), palette='mako')
plt.title('Top Selling Products')
plt.xlabel('Product ID')
plt.ylabel('Total Sales')
plt.show()

# Visualize Top Customers
plt.figure(figsize=(12, 6))
sns.barplot(x='customer_id', y='total_sales', data=top_customers.head(10), palette='rocket')
plt.title('Top Customers')
plt.xlabel('Customer ID')
plt.ylabel('Total Sales')
plt.show()

# Visualize Monthly Sales Trends
plt.figure(figsize=(12, 6))
sns.lineplot(x='order_date', y='total_sales', data=monthly_sales_trends, marker='o', palette='coolwarm')
plt.title('Monthly Sales Trends')
plt.xlabel('Order Date')
plt.ylabel('Total Sales')
plt.xticks(rotation=45)
plt.show()

# Visualize Average Sales per Weather Condition
plt.figure(figsize=(12, 6))
sns.barplot(x='weather_description', y='total_sales', data=average_sales_per_weather, palette='pastel')
plt.title('Average Sales per Weather Condition')
plt.xlabel('Weather Description')
plt.ylabel('Average Sales')
plt.xticks(rotation=45, ha='right')
plt.show()
