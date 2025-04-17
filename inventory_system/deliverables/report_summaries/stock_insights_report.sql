-- Query to get total quantity of each product:
SELECT
    product_id,
    product_name,
    SUM(stock_quantity) AS total_stock
FROM
    products
GROUP BY
    product_id, product_name
ORDER BY
    product_name;