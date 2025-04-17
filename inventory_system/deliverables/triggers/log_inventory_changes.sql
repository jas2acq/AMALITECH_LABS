DELIMITER ;;
CREATE TRIGGER `log_inventory_changes` AFTER UPDATE ON `products` FOR EACH ROW BEGIN
  INSERT INTO inventory_logs (product_id, change_type, quantity_changed, previous_stock, new_stock, remarks)
  VALUES (
    NEW.product_id,
    CASE
        WHEN NEW.stock_quantity < OLD.stock_quantity THEN 'Order'
        WHEN NEW.stock_quantity > OLD.stock_quantity THEN 'Restock'
        ELSE 'Adjustment'
    END,
    ABS(NEW.stock_quantity - OLD.stock_quantity),
    OLD.stock_quantity,
    NEW.stock_quantity,
    'Stock updated automatically'
  );
END ;;
DELIMITER ;