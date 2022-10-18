DROP TABLE IF EXISTS prod;
DROP TABLE IF EXISTS qc;
DROP TABLE IF EXISTS res;
DROP TABLE IF EXISTS rel;

CREATE TABLE qc AS
SELECT companyname,`order`.id,orderdate
FROM customer JOIN `order` ON customer.id = customerid
WHERE companyname = 'Queen Cozinha';

CREATE TABLE prod AS
SELECT orderid,productid
FROM qc,orderdetail
WHERE qc.id = orderid AND orderdate like '2014-12-25%';

CREATE TABLE res AS
SELECT product.id,productname
FROM prod JOIN product ON productid = product.id
ORDER BY product.id;

CREATE TABLE rel AS
SELECT id,lead(id, 1, -1) OVER (ORDER BY id) AS nextid,productname
FROM res;

WITH RECURSIVE
    cte AS (
        SELECT id,nextid,productname FROM rel WHERE productname = 'Mishi Kobe Niku'
        UNION ALL
        SELECT rel.id,
                rel.nextid,
                cte.productname || ',' || rel.productname 
        FROM rel,cte 
        WHERE rel.id = cte.nextid
    )
SELECT productname 
FROM cte
WHERE cte.nextid = -1;

DROP TABLE prod;
DROP TABLE qc;
DROP TABLE res;
DROP TABLE rel;
