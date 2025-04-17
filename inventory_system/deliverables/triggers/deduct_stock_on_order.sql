DELIMITER ;;
CREATE TRIGGER `deduct_stock_on_order` AFTER INSERT ON `order_details` FOR EACH ROW BEGIN
  UPDATE products
  SET stock_quantity = stock_quantity - NEW.quantity
  WHERE product_id = NEW.product_id;
END ;;
DELIMITER ;