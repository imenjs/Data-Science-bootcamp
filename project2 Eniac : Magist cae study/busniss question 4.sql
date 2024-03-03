-- what’s the average time between the order being placed and the product being delivered?

SELECT
    AVG(TIMESTAMPDIFF(DAY, order_purchase_timestamp, order_delivered_customer_date)) AS average_delivery_time_in_days
FROM
   orders ;
   

-- How many orders are delivered on time vs orders delivered with a delay?
SELECT 
    COUNT(order_id) AS orders,
    CASE 
        WHEN timestampdiff(day, order_delivered_customer_date, order_estimated_delivery_date) >= 0 THEN 'DELIVERED ON TIME'
        ELSE 'DELIVERED DELAY'
    END AS delays
FROM ORDERS
GROUP BY delays;


-- total number of delevery
select count(*) from orders ;

-- number of delayed delevery 
SELECT
    COUNT(*) AS delayed_orders
FROM
    orders
WHERE
    order_delivered_customer_date > order_estimated_delivery_date;

-- percentage of delayed delevery = 7.874%



-- Is there any pattern for delayed orders, e.g. big products being delayed more often?
SELECT *
FROM products
LEFT JOIN order_items ON products.product_id = order_items.product_id
LEFT JOIN orders ON order_items.order_id = orders.order_id
WHERE order_delivered_customer_date > order_estimated_delivery_date ;

-- check average product weight 
select avg(product_weight_g) from products;

select avg(product_weight_g) from products
LEFT JOIN order_items ON products.product_id = order_items.product_id
LEFT JOIN orders ON order_items.order_id = orders.order_id
WHERE order_delivered_customer_date > order_estimated_delivery_date ;