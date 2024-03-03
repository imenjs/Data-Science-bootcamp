SELECT
    AVG(average_order_value) AS overall_average_order_value
FROM (
    SELECT
        order_id,
        AVG(price) AS average_order_value
    FROM
        order_items
    GROUP BY
        order_id
) AS order_avg;