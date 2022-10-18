DROP TABLE IF EXISTS et;
DROP TABLE IF EXISTS ett;
DROP TABLE IF EXISTS tr;

CREATE TABLE et AS
SELECT firstname,lastname,birthdate,territoryid AS tid
FROM employee JOIN employeeterritory ON employee.id = employeeid;

CREATE TABLE ett AS
SELECT firstname,lastname,birthdate,regionid AS rid
FROM et JOIN territory ON tid = territory.id;

CREATE TABLE tr AS
SELECT region.id AS region_id,regiondescription,firstname,lastname,birthdate
FROM ett JOIN region ON rid = region.id;

SELECT regiondescription,firstname,lastname,max(birthdate) AS yongest
FROM tr
GROUP BY region_id
ORDER BY region_id;

DROP TABLE et;
DROP TABLE ett;
DROP TABLE tr;
