DELIMITER ;;
CREATE TRIGGER `alert_low_stock` 
AFTER UPDATE ON `products` 
FOR EACH ROW 
BEGIN IF NEW.stock_quantity < NEW.reorder_level 
THEN INSERT INTO inventory_logs (product_id, change_type, quantity_changed, previous_stock, new_stock, remarks) 
VALUES (NEW.product_id, 'Adjustment', 0, OLD.stock_quantity, NEW.stock_quantity, 'Low stock warning - Consider restocking');
END IF;
END ;;
DELIMITER ;