use magist ;

-- What categories of tech products does Magist have?
select distinct product_category_name_english
from product_category_name_translation ;
 

-- How many products of these tech categories have been sold (within the time window of the database snapshot)? What percentage does that represent from the overall number of products sold?

select distinct product_category_name_english, count(order_items.product_id)
from product_category_name_translation
left join products 
on product_category_name_translation.product_category_name = products.product_category_name 
left join order_items 
on products.product_id=order_items.product_id 
left join orders 
on order_items.order_id = orders.order_id  
where product_category_name_english in ( "telephony", "watches_gifts", "computers", "pc_gamers","computers_accessories", "electronics","consoles_games") and  order_purchase_timestamp between '2017-04-01' and '2018-03-01'
group by product_category_name_english ;

SELECT
    product_category_name_english,
    COUNT(order_items.product_id) AS total_count,
    ROUND((COUNT(order_items.product_id) / (SELECT COUNT(*) FROM order_items)) * 100, 2) AS percentage
FROM
    product_category_name_translation
LEFT JOIN
    products ON product_category_name_translation.product_category_name = products.product_category_name
LEFT JOIN
    order_items ON products.product_id = order_items.product_id
    LEFT JOIN
    orders ON order_items.order_id = orders.order_id
WHERE
    product_category_name_english IN ('telephony', 'computers', 'watches_gifts', 'pc_gamers', 'computers_accessories', 'electronics', 'consoles_games') and  orders.order_purchase_timestamp between '2017-04-01' and '2018-03-01'
GROUP BY
    product_category_name_english;



-- What’s the average price of the products being sold?

SELECT
    product_category_name_english,
    COUNT(order_items.product_id) AS total_count,
    ROUND((COUNT(order_items.product_id) / (SELECT COUNT(*) FROM order_items)) * 100, 2) AS count_product_percentage,
    AVG(order_items.price) AS average_price
    
FROM
    product_category_name_translation
LEFT JOIN
    products ON product_category_name_translation.product_category_name = products.product_category_name
LEFT JOIN
    order_items ON products.product_id = order_items.product_id
WHERE
    product_category_name_english IN ('telephony', 'computers', 'watches_gifts', 'pc_gamers', 'computers_accessories', 'electronics', 'consoles_games')
GROUP BY
    product_category_name_english;
-- Are expensive tech products popular? *
-- * TIP: Look at the function CASE WHEN to accomplish this task.
SELECT
    product_category_name_english,
    COUNT(order_items.product_id) AS total_count,
    AVG(order_items.price) AS average_price,
    CASE
        WHEN AVG(order_items.price) > 200 THEN 'Expensive'
        ELSE 'Not Expensive'
    END AS Expensive ,
    
    CASE
        WHEN COUNT(order_items.product_id) > 2000 then 'popular'
        ELSE 'not popular'
    END AS popularity
FROM
    product_category_name_translation
LEFT JOIN
    products ON product_category_name_translation.product_category_name = products.product_category_name
LEFT JOIN
    order_items ON products.product_id = order_items.product_id
WHERE
    product_category_name_english IN (  'computers', 'watches_gifts', 'pc_gamers', 'computers_accessories', 'electronics', 'consoles_games')
GROUP BY
    product_category_name_english
    order by average_price DESC;