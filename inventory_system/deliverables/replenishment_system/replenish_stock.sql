DELIMITER //
CREATE PROCEDURE replenish_stock (
    IN product_id_val VARCHAR(50),
    IN quantity_added INT,
    IN remarks_text VARCHAR(255)
)
BEGIN
    -- Update the stock quantity
    UPDATE products
    SET stock_quantity = stock_quantity + quantity_added
    WHERE product_id = product_id_val;

    -- The log_inventory_changes trigger will automatically log this change as 'Restock'
END //
DELIMITER ;