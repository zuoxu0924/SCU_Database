DROP TABLE IF EXISTS allinfo;
DROP TABLE IF EXISTS lateinfo;
DROP TABLE IF EXISTS dp;

CREATE TABLE allinfo AS
SELECT shipvia AS aid,count(*) AS alltrans
FROM `order`
GROUP BY shipvia;

CREATE TABLE lateinfo AS
SELECT shipvia AS lid,count(*) AS latetrans
FROM `order`
WHERE shippeddate > requireddate
GROUP BY shipvia;

CREATE TABLE dp AS
SELECT aid,round(((lateinfo.latetrans * 1.0) / (1.0 * allinfo.alltrans)) * 100, 2) AS delaypercent
FROM allinfo JOIN lateinfo ON lateinfo.lid = allinfo.aid;

SELECT companyname,delaypercent
FROM dp JOIN shipper ON dp.aid = shipper.id;

DROP TABLE dp;
DROP TABLE lateinfo;
DROP TABLE allinfo;
