CREATE VIEW order_summary_view AS
SELECT
    c.customer_name,
    o.order_id,
    o.order_date,
    o.total_amount,
    COUNT(od.product_id) AS number_of_items
FROM
    orders o
JOIN
    customers c ON o.customer_id = c.customer_id
JOIN
    order_details od ON o.order_id = od.order_id
GROUP BY
    c.customer_name, o.order_id, o.order_date, o.total_amount;