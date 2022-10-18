DROP TABLE IF EXISTS sortedshipname;
DROP TABLE IF EXISTS hyphenpos;

CREATE TABLE sortedshipname AS
SELECT DISTINCT shipname
FROM `order`
WHERE shipname like '%-%';

CREATE TABLE hyphenpos AS
SELECT shipname,instr(shipname, '-') AS pos
FROM sortedshipname;

SELECT shipname,substr(shipname, 1, pos - 1) AS subshipname
FROM hyphenpos
ORDER BY shipname;

DROP TABLE hyphenpos;
DROP TABLE sortedshipname;
