use magist;

-- 1. How many orders are there in the dataset?
select  count( order_item_id) from order_items ;
-- 99441

-- 2. Are orders actually delivered?

Select order_status ,count(order_status) 
from orders
group by order_status;
-- 3. Is Magist having user growth?
select * from orders;
select
 month(order_purchase_timestamp) as month ,
 year(order_purchase_timestamp) as year , count(customer_id) from orders 
 group by  year, month
 order by year, month;
 
 
 


-- 4. How many products are there in the products table?
select * from products ;
select count(distinct product_id )from products;

-- 5. Which are the categories with most products?
select count(distinct product_id ) as count_order , product_category_name from products 
group by product_category_name
order by  count_order DESC; 

Select * from product_category_name_translation ;

-- 6. How many of those products were present in actual transactions?
select count(distinct product_id) from products;
select count(distinct product_id) from order_items;




-- 7. What’s the price for the most expensive and cheapest products?

Select max(price) , min(price) from order_items;

-- 8. What are the highest and lowest payment values?order_payments
Select max(payment_value) , min(payment_value) from order_payments;
select * from order_payments;
select sum(payment_value) as total from order_payments
group by order_id
order by total DESC ;

select avg(price) from order_items ;
select count( customer_id) from customers;