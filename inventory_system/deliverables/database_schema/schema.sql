-- Table structure for table `customers`
CREATE TABLE IF NOT EXISTS `customers` (
  `customer_id` int NOT NULL AUTO_INCREMENT,
  `customer_name` varchar(255) NOT NULL,
  `age` int DEFAULT NULL,
  `gender` enum('Male','Female','Other') NOT NULL,
  `country` varchar(100) NOT NULL,
  `email` varchar(255) NOT NULL,
  `phone` varchar(20) NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`customer_id`),
  UNIQUE KEY `email` (`email`),
  UNIQUE KEY `phone` (`phone`),
  CONSTRAINT `customers_chk_1` CHECK ((`age` >= 0))
) ENGINE=InnoDB AUTO_INCREMENT=21 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



-- Table structure for table `inventory_logs`
CREATE TABLE IF NOT EXISTS `inventory_logs` (
  `log_id` int NOT NULL AUTO_INCREMENT,
  `product_id` varchar(10) NOT NULL,
  `change_type` enum('Order','Restock','Adjustment') NOT NULL,
  `quantity_changed` int NOT NULL,
  `previous_stock` int NOT NULL,
  `new_stock` int NOT NULL,
  `timestamp` datetime DEFAULT CURRENT_TIMESTAMP,
  `remarks` text,
  PRIMARY KEY (`log_id`),
  KEY `product_id` (`product_id`),
  CONSTRAINT `inventory_logs_ibfk_1` FOREIGN KEY (`product_id`) REFERENCES `products` (`product_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=64 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



-- Table structure for table `order_details`
CREATE TABLE IF NOT EXISTS `order_details` (
  `order_detail_id` int NOT NULL AUTO_INCREMENT,
  `order_id` int NOT NULL,
  `product_id` varchar(10) NOT NULL,
  `quantity` int NOT NULL,
  `price` decimal(10,2) NOT NULL,
  PRIMARY KEY (`order_detail_id`),
  KEY `order_id` (`order_id`),
  KEY `product_id` (`product_id`),
  CONSTRAINT `order_details_ibfk_1` FOREIGN KEY (`order_id`) REFERENCES `orders` (`order_id`) ON DELETE CASCADE,
  CONSTRAINT `order_details_ibfk_2` FOREIGN KEY (`product_id`) REFERENCES `products` (`product_id`),
  CONSTRAINT `order_details_chk_1` CHECK ((`quantity` > 0))
) ENGINE=InnoDB AUTO_INCREMENT=64 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



-- Table structure for table `orders`
CREATE TABLE IF NOT EXISTS `orders` (
  `order_id` int NOT NULL AUTO_INCREMENT,
  `customer_id` int NOT NULL,
  `order_date` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `total_amount` decimal(10,2) DEFAULT '0.00',
  PRIMARY KEY (`order_id`),
  KEY `customer_id` (`customer_id`),
  CONSTRAINT `orders_ibfk_1` FOREIGN KEY (`customer_id`) REFERENCES `customers` (`customer_id`)
) ENGINE=InnoDB AUTO_INCREMENT=51 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



-- Table structure for table `products`
CREATE TABLE IF NOT EXISTS `products` (
  `product_id` varchar(10) NOT NULL,
  `product_name` varchar(255) NOT NULL,
  `category` varchar(100) DEFAULT NULL,
  `price` decimal(10,2) NOT NULL,
  `stock_quantity` int NOT NULL,
  `reorder_level` int NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;