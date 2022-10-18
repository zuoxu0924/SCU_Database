DROP TABLE IF EXISTS statistcs;

CREATE TABLE statistcs AS
SELECT categoryid,
        count(categoryid) AS total_items,
        round(avg(unitprice), 2) AS average_price,
        min(unitprice) AS minimum_price,
        max(unitprice) AS maximum_price,
        sum(unitsonorder) AS total_on_order
FROM product
GROUP BY categoryid;

--SELECT * FROM statistcs;

SELECT categoryname,
        total_items,
        average_price,
        minimum_price,
        maximum_price,
        total_on_order
FROM category JOIN statistcs ON category.id = statistcs.categoryid
WHERE total_items > 10
ORDER BY category.id;

DROP TABLE statistcs;
