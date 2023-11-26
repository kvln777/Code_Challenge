
CREATE TABLE sales_data (
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity INT,
    price DECIMAL(10, 2),
    order_date DATE
)
CREATE TABLE sales_geo_info (
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity INT,
    price DECIMAL(10, 2),
    order_date DATETIME,
    name NVARCHAR(255),
    username NVARCHAR(255),
    email NVARCHAR(255),
    lat FLOAT,
    lng FLOAT,
	CONSTRAINT fk_sales_data_geo_info FOREIGN KEY (order_id) REFERENCES sales_data(order_id)
);


CREATE TABLE sales_geo_info_final (
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity INT,
    price DECIMAL(10, 2),
    order_date DATETIME,
    name NVARCHAR(255),
    username NVARCHAR(255),
    email NVARCHAR(255),
    lat FLOAT,
    lng FLOAT,
    temperature FLOAT,
    weather_description NVARCHAR(255),
	CONSTRAINT fk_sales_data_final FOREIGN KEY (order_id) REFERENCES sales_data(order_id),
    CONSTRAINT fk_sales_geo_info_final FOREIGN KEY (order_id) REFERENCES sales_geo_info(order_id)
);
