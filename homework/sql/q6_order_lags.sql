DROP TABLE IF EXISTS orilags;

CREATE TABLE orilags AS
SELECT id,orderdate,lag(orderdate, 1, orderdate) OVER (ORDER BY orderdate) AS pre_orderdate,
        julianday(orderdate) - julianday(lag(orderdate, 1, orderdate) OVER (ORDER BY orderdate)) AS orderlags
FROM `order`
WHERE customerid = 'BLONP'                 
LIMIT 10;

SELECT id,orderdate,pre_orderdate,round(orderlags, 2) AS lagtime
FROM orilags
ORDER BY orderdate;

DROP TABLE orilags;
