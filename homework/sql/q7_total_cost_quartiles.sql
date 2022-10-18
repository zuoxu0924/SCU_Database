DROP TABLE IF EXISTS info;
DROP TABLE IF EXISTS te;

CREATE TABLE te AS
SELECT customerid,round(sum(unitprice * quantity), 2) AS totalexpenditures
FROM `order` JOIN orderdetail ON `order`.id = orderid
GROUP BY customerid;

CREATE TABLE info AS
SELECT ifnull(companyname, 'MISSING_NAME') AS companyname,customerid,totalexpenditures
FROM te LEFT OUTER JOIN customer ON te.customerid = customer.id
ORDER BY totalexpenditures;

SELECT companyname,customerid,totalexpenditures
FROM (SELECT *,
        ntile(4) OVER (ORDER BY totalexpenditures) AS group_num
    FROM info
)
WHERE group_num = 1;

DROP TABLE info;
DROP TABLE te;