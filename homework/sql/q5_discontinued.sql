DROP TABLE IF EXISTS disprod;
DROP TABLE IF EXISTS disdetail;
DROP TABLE IF EXISTS orderedbydate;

CREATE TABLE disprod AS
SELECT id AS pid,productname
FROM product
WHERE discontinued = '1';

CREATE TABLE disdetail AS
SELECT orderid,productname
FROM disprod JOIN orderdetail ON pid = productid;

DROP TABLE disprod;

CREATE TABLE orderedbydate AS
SELECT orderid,customerid,productname,min(orderdate) AS minimum_date
FROM disdetail JOIN `order` ON orderid = id
GROUP BY orderdate;

DROP TABLE disdetail;

CREATE TABLE firstperson AS
SELECT orderid,productname,customerid,min(minimum_date) AS pmin
FROM orderedbydate
GROUP BY productname
ORDER BY productname;

DROP TABLE orderedbydate;

SELECT productname,companyname,contactname
FROM firstperson JOIN customer ON customerid = customer.id
ORDER BY productname;

DROP TABLE firstperson;
