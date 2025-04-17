CREATE VIEW low_stock_products_view AS
SELECT
    p.product_id,
    p.product_name,
    p.stock_quantity,
    p.reorder_level
FROM
    products p
WHERE
    p.stock_quantity < p.reorder_level;