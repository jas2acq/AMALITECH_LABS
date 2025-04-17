DELIMITER ;;
CREATE TRIGGER `update_order_total` 
AFTER INSERT ON `order_details` 
FOR EACH ROW BEGIN UPDATE orders 
SET total_amount = (SELECT SUM(quantity * price) FROM order_details WHERE order_id = NEW.order_id)
WHERE order_id = NEW.order_id;
END ;;
DELIMITER ;