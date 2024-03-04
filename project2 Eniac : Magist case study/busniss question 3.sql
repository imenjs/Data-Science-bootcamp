-- How many months of data are included in the magist database?
use magist ;
select * from orders;

SELECT
    MIN(order_purchase_timestamp) AS earliest_date,
    MAX(order_purchase_timestamp) AS latest_date,
    TIMESTAMPDIFF(MONTH,  MIN(order_purchase_timestamp) , MAX(order_purchase_timestamp)) AS months_of_data
FROM
    orders;
    
  
    
-- How many sellers are there? How many Tech sellers are there? What percentage of overall sellers are Tech sellers?
select count(seller_id) 
from sellers;

SELECT
    product_category_name_english,
    COUNT(order_items.seller_id) AS seller_count,
     (COUNT(DISTINCT order_items.seller_id) / (SELECT COUNT(DISTINCT sellers.seller_id) FROM sellers)) * 100 AS seller_percentage
FROM
    product_category_name_translation
LEFT JOIN
    products ON product_category_name_translation.product_category_name = products.product_category_name
LEFT JOIN
    order_items ON products.product_id = order_items.product_id
WHERE
    product_category_name_english IN ( 'telephony', 'computers', 'watches_gifts', 'pc_gamers', 'computers_accessories', 'electronics', 'consoles_games')
GROUP BY
    product_category_name_english;

-- What is the total amount earned by all sellers? What is the total amount earned by all Tech sellers?

-- Total fo tech seller
SELECT
    SUM(order_items.price) AS total_earnings
FROM
    order_items
JOIN
    products ON order_items.product_id = products.product_id
JOIN
    product_category_name_translation ON products.product_category_name = product_category_name_translation.product_category_name 
 WHERE
 product_category_name_english IN ( 'telephony', 'computers', 'watches_gifts', 'pc_gamers', 'computers_accessories', 'electronics', 'consoles_games');

-- earning per category
SELECT
    product_category_name_english,
    SUM(order_items.price) AS total_earnings,
    (SUM(order_items.price) / (SELECT SUM(order_items.price) FROM order_items)) * 100 AS earnings_percentage
FROM
    product_category_name_translation
LEFT JOIN
    products ON product_category_name_translation.product_category_name = products.product_category_name
LEFT JOIN
    order_items ON products.product_id = order_items.product_id
WHERE
    product_category_name_english IN ( 'telephony', 'computers', 'watches_gifts', 'pc_gamers', 'computers_accessories', 'electronics', 'consoles_games')
GROUP BY
    product_category_name_english 
Order by  total_earnings DESC;
-- Can you work out the average monthly income of all sellers? Can you work out the average monthly income of Tech sellers?

-- Average Monthly Income of All Sellers
SELECT
    AVG(order_items.price) AS average_monthly_income_all_sellers
FROM
    order_items;

-- Average Monthly Income of Tech Sellers
SELECT
    AVG(order_items.price) AS average_monthly_income_tech_sellers
FROM
    product_category_name_translation
JOIN
    products ON product_category_name_translation.product_category_name = products.product_category_name
JOIN
    order_items ON products.product_id = order_items.product_id
WHERE
    product_category_name_english IN ( 'telephony', 'computers', 'watches_gifts', 'pc_gamers', 'audio', 'computers_accessories', 'electronics', 'consoles_games');
