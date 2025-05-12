CREATE TABLE IF NOT EXISTS purchase_data (
    customer_id VARCHAR(50),       
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INT,
    order_id VARCHAR(50),     
    time_stamp TIMESTAMP,
    product_id VARCHAR(50),
    product_name VARCHAR(100),
    quantity INT,
    unit_price FLOAT
);