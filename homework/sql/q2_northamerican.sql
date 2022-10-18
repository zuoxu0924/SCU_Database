DROP TABLE IF EXISTS temp;

CREATE TABLE temp AS
SELECT id,shipcountry,shipregion
FROM `order`
ORDER BY id
LIMIT 20 OFFSET 15445 - (SELECT min(id) FROM `order`);

UPDATE temp
SET shipregion = 'OtherPlace'
WHERE shipcountry <> 'USA' or shipcountry <> 'Mexico' or shipcountry <> 'Canada';

UPDATE temp
SET shipregion = 'NorthAmerica'
WHERE shipcountry = 'USA' or shipcountry = 'Mexico' or shipcountry = 'Canada';

SELECT *
FROM temp;

DROP TABLE temp;
