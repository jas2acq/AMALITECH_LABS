DELIMITER //
CREATE PROCEDURE categorize_customers()
BEGIN
    -- Define your spending categories and thresholds
    DECLARE high_spender_threshold DECIMAL(10, 2) DEFAULT 1000.00;
    DECLARE medium_spender_threshold DECIMAL(10, 2) DEFAULT 500.00;


    -- Update customer categories
    UPDATE customers c
    SET spending_category = 'High Spender'
    WHERE c.customer_id IN (
        SELECT o.customer_id
        FROM orders o
        GROUP BY o.customer_id
        HAVING SUM(o.total_amount) > high_spender_threshold
    );

    UPDATE customers c
    SET spending_category = 'Medium Spender'
    WHERE c.customer_id IN (
        SELECT o.customer_id
        FROM orders o
        GROUP BY o.customer_id
        HAVING SUM(o.total_amount) > medium_spender_threshold
           AND SUM(o.total_amount) <= high_spender_threshold
    );

    UPDATE customers c
    SET spending_category = 'Low Spender'
    WHERE c.customer_id IN (
        SELECT o.customer_id
        FROM orders o
        GROUP BY o.customer_id
        HAVING SUM(o.total_amount) <= medium_spender_threshold
    );

    -- Adding a category for new customers
    UPDATE customers
    SET spending_category = 'New Customer'
    WHERE spending_category IS NULL;

END //
DELIMITER ;