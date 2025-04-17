DELIMITER //
CREATE PROCEDURE replenish_stock (
    IN product_id_val INT,
    IN quantity_added INT,
    IN remarks_text VARCHAR(255)
)
BEGIN
    -- Update the stock quantity
    UPDATE products
    SET stock_quantity = stock_quantity + quantity_added
    WHERE product_id = product_id_val;

    -- The log_inventory_changes trigger will automatically log this change as 'Restock'

    -- Optionally, insert a more detailed log entry
    INSERT INTO inventory_logs (product_id, change_type, quantity_changed, previous_stock, new_stock, remarks)
    SELECT
        p.product_id,
        'Restock',
        quantity_added,
        p.stock_quantity - quantity_added,
        p.stock_quantity,
        remarks_text
    FROM products p
    WHERE p.product_id = product_id_val;

END //
DELIMITER ;