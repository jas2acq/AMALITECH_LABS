-- Query to retrieve order summaries:
SELECT
    o.order_id,
    c.customer_name,
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
    o.order_id, c.customer_name, o.order_date, o.total_amount
ORDER BY
    o.order_date DESC;