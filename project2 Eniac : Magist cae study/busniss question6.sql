WITH MonthlyCustomerCounts AS (
    SELECT
        YEAR(order_purchase_timestamp) AS year,
        MONTH(order_purchase_timestamp) AS month,
        COUNT(DISTINCT customer_id) AS customer_count
    FROM
        orders
    GROUP BY
        YEAR(order_purchase_timestamp),
        MONTH(order_purchase_timestamp)
)

SELECT
    m1.year,
    m1.month,
    m1.customer_count,
    LAG(m1.customer_count) OVER (ORDER BY m1.year, m1.month) AS prev_month_customer_count,
    CASE
        WHEN LAG(m1.customer_count) OVER (ORDER BY m1.year, m1.month) > 0 THEN
            ((m1.customer_count - LAG(m1.customer_count) OVER (ORDER BY m1.year, m1.month)) / LAG(m1.customer_count) OVER (ORDER BY m1.year, m1.month)) * 100
        ELSE
            0
    END AS growth_percentage
FROM
    MonthlyCustomerCounts m1
ORDER BY
    m1.year, m1.month;




# the average growth 
WITH MonthlyCustomerCounts AS (
    SELECT
        YEAR(order_purchase_timestamp) AS year,
        MONTH(order_purchase_timestamp) AS month,
        COUNT(DISTINCT customer_id) AS customer_count
    FROM
        orders
    GROUP BY
        YEAR(order_purchase_timestamp),
        MONTH(order_purchase_timestamp)
)

SELECT
    AVG(growth_percentage) AS average_growth_percentage
FROM (
    SELECT
        m1.year,
        m1.month,
        m1.customer_count,
        LAG(m1.customer_count) OVER (ORDER BY m1.year, m1.month) AS prev_month_customer_count,
        CASE
            WHEN LAG(m1.customer_count) OVER (ORDER BY m1.year, m1.month) > 0 THEN
                ((m1.customer_count - LAG(m1.customer_count) OVER (ORDER BY m1.year, m1.month)) / LAG(m1.customer_count) OVER (ORDER BY m1.year, m1.month)) * 100
            ELSE
                0
        END AS growth_percentage
    FROM
        MonthlyCustomerCounts m1
) AS growth_data;

# growth from 01-2017 to 06-2018

WITH MonthlyCustomerCounts AS (
    SELECT
        YEAR(order_purchase_timestamp) AS year,
        MONTH(order_purchase_timestamp) AS month,
        COUNT(DISTINCT customer_id) AS customer_count
    FROM
        orders
    WHERE
        order_purchase_timestamp >= '2017-01-01' AND order_purchase_timestamp < '2018-07-01'
    GROUP BY
        YEAR(order_purchase_timestamp),
        MONTH(order_purchase_timestamp)
)

SELECT
    AVG(growth_percentage) AS average_growth_percentage
FROM (
    SELECT
        m1.year,
        m1.month,
        m1.customer_count,
        LAG(m1.customer_count) OVER (ORDER BY m1.year, m1.month) AS prev_month_customer_count,
        CASE
            WHEN LAG(m1.customer_count) OVER (ORDER BY m1.year, m1.month) > 0 THEN
                ((m1.customer_count - LAG(m1.customer_count) OVER (ORDER BY m1.year, m1.month)) / LAG(m1.customer_count) OVER (ORDER BY m1.year, m1.month)) * 100
            ELSE
                0
        END AS growth_percentage
    FROM
        MonthlyCustomerCounts m1
) AS growth_data;
