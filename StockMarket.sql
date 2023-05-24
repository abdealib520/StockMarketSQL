-- Creating the two tables
CREATE TABLE abdeali9.stockmarketprice(
   DATE         DATE 
  ,SYMBOL       VARCHAR(10) NOT NULL
  ,CLOSE   		double
  ,PREV_CLOSE   double
  ,INDEX (DATE,SYMBOL,CLOSE)
);

CREATE TABLE abdeali9.stockmarkettrade(
   DATE         DATE 
  ,SYMBOL       VARCHAR(10) NOT NULL
  ,VOLUME       double
  ,NO_OF_TRADES double
  ,INDEX (DATE,SYMBOL,VOLUME)
);

-- Allowing SQL to read from local data
SET GLOBAL local_infile=1;

-- Reading from Local Data
LOAD DATA LOCAL INFILE 'C:/Users/abdea/Documents/Abdeali_Python/StockMarketPrice.csv' INTO TABLE abdeali9.stockmarketprice
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' LINES 
TERMINATED BY '\n'
IGNORE 1 LINES ;

LOAD DATA LOCAL INFILE 'C:/Users/abdea/Documents/Abdeali_Python/StockMarketTrade.csv' INTO TABLE abdeali9.stockmarkettrade
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' LINES 
TERMINATED BY '\n'
IGNORE 1 LINES ;



-- The highest price of a stock along with the date
SELECT p.SYMBOL, p.DATE,
p.CLOSE as Highest_Price
FROM abdeali9.stockmarketprice AS p
INNER JOIN (
SELECT SYMBOL, max(CLOSE) AS max_price
FROM abdeali9.stockmarketprice
GROUP BY SYMBOL
) AS subq
ON p.SYMBOL=subq.SYMBOL AND p.CLOSE=subq.max_price;


-- The Lowest price of a stock along with the date
SELECT p.SYMBOL, p.DATE,
p.CLOSE as Lowest_Price
FROM abdeali9.stockmarketprice AS p
INNER JOIN (
SELECT SYMBOL, min(CLOSE) AS min_price
FROM abdeali9.stockmarketprice
GROUP BY SYMBOL
) AS subq
ON p.SYMBOL=subq.SYMBOL AND p.CLOSE=subq.min_price;


--  Difference between highest and lowest stock price
SELECT SYMBOL, max(CLOSE) - min(CLOSE) as Difference_In_Price
FROM abdeali9.stockmarketprice
GROUP BY SYMBOL;

-- Total Value circulated per stock per day
-- Will be off by a couple percent as price of stock fluctuates during a day
SELECT p.SYMBOL, p.DATE, p.CLOSE*t.VOLUME AS VALUE
FROM abdeali9.stockmarketprice AS p
INNER JOIN abdeali9.stockmarkettrade AS t
ON p.SYMBOL=t.SYMBOL AND t.DATE=p.DATE;


-- Daily Returns of a All Stocks
SELECT SYMBOL, DATE, CLOSE,
LAG(CLOSE) OVER(PARTITION BY SYMBOL ORDER BY DATE) AS PREV_CLOSE,
CLOSE - LAG(CLOSE) OVER(PARTITION BY SYMBOL ORDER BY DATE) AS DAILY_RETURN
FROM abdeali9.stockmarketprice;

-- 10 Day Moving Average
SELECT SYMBOL, DATE,
AVG(CLOSE) OVER(PARTITION BY SYMBOL ORDER BY DATE ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS Moving_Average
FROM abdeali9.stockmarketprice;


-- Calculate 52 Week High and 52 Week Low
-- Query takes too long to compute, see the procedure
SELECT SYMBOL, DATE,
MAX(CLOSE) OVER(PARTITION BY SYMBOL ORDER BY DATE ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS 52Week_High
,MIN(CLOSE) OVER(PARTITION BY SYMBOL ORDER BY DATE ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS 52Week_Low
FROM abdeali9.stockmarketprice;




-- Stored Procedures
-- Highest Price of Stock during specific time
DELIMITER //
CREATE PROCEDURE Highest_Price_Stock(stock text,start_date date,end_date date)
BEGIN
SELECT p.SYMBOL, p.DATE,
p.CLOSE as Highest_Price
FROM abdeali9.stockmarketprice AS p
INNER JOIN (
SELECT SYMBOL, max(CLOSE) AS max_price
FROM abdeali9.stockmarketprice
WHERE SYMBOL=stock AND
DATE BETWEEN start_date AND end_date
GROUP BY SYMBOL
) AS subq
ON p.SYMBOL=subq.SYMBOL AND p.CLOSE=subq.max_price;
END
//
DELIMITER ;
CALL Highest_Price_Stock('3MINDIA','2010-01-01','2015-01-01');


-- Lowest Price of Stock during specific time
DELIMITER //
CREATE PROCEDURE Lowest_Price_Stock(stock text,start_date date,end_date date)
BEGIN
SELECT p.SYMBOL, p.DATE,
p.CLOSE as Lowest_Price
FROM abdeali9.stockmarketprice AS p
INNER JOIN (
SELECT SYMBOL, min(CLOSE) AS min_price
FROM abdeali9.stockmarketprice
WHERE SYMBOL=stock AND
DATE BETWEEN start_date AND end_date
GROUP BY SYMBOL
) AS subq
ON p.SYMBOL=subq.SYMBOL AND p.CLOSE=subq.min_price;
END
//
DELIMITER ;
CALL Lowest_Price_Stock('3MINDIA','2010-01-01','2015-01-01');


-- Highest And Lowest in one go with date and during specific time
-- First row will contain Highest and second row will contain Lowest
DELIMITER //
CREATE PROCEDURE Highest_Lowest_Price_Stock(stock text,start_date date,end_date date)
BEGIN
SELECT p.SYMBOL, p.DATE,
p.CLOSE as Price
FROM abdeali9.stockmarketprice AS p
INNER JOIN (
SELECT SYMBOL, min(CLOSE) AS price
FROM abdeali9.stockmarketprice
WHERE SYMBOL=stock AND
DATE BETWEEN start_date AND end_date
GROUP BY SYMBOL
UNION
SELECT SYMBOL, max(CLOSE) AS price
FROM abdeali9.stockmarketprice
WHERE SYMBOL=stock AND
DATE BETWEEN start_date AND end_date
GROUP BY SYMBOL
) AS subq
ON p.SYMBOL=subq.SYMBOL AND p.CLOSE=subq.price;
END
//
DELIMITER ;
CALL Highest_Lowest_Price_Stock('3MINDIA','2010-01-01','2015-01-01');

-- Value circulated of that stock in specific time per day
DELIMITER //
CREATE PROCEDURE value(stock text,start_date date,end_date date)
BEGIN
SELECT p.SYMBOL, p.DATE, p.CLOSE*t.VOLUME AS VALUE
FROM abdeali9.stockmarketprice AS p
INNER JOIN abdeali9.stockmarkettrade AS t
ON p.SYMBOL=t.SYMBOL AND t.DATE=p.DATE
WHERE p.SYMBOL=stock AND p.DATE BETWEEN start_date AND end_date;
END
//
DELIMITER ;
CALL value('3MINDIA','2010-01-01','2010-01-20');


-- Daily Return of stock during specific time
DELIMITER //
CREATE PROCEDURE daily_returns(stock text,start_date date,end_date date)
BEGIN
SELECT SYMBOL, DATE, CLOSE,
LAG(CLOSE) OVER(PARTITION BY SYMBOL ORDER BY DATE) AS PREV_CLOSE,
CLOSE - LAG(CLOSE) OVER(PARTITION BY SYMBOL ORDER BY DATE) AS DAILY_RETURN
FROM abdeali9.stockmarketprice
WHERE SYMBOL=stock AND DATE BETWEEN start_date AND end_date;
END
//
DELIMITER //
CALL daily_returns('3MINDIA','2010-01-01','2010-01-30');


-- 10 Day Moving Average of Stock during specifc time
DELIMITER //
CREATE PROCEDURE moving_average_10(stock text,start_date date,end_date date)
BEGIN
SELECT SYMBOL, DATE,
AVG(CLOSE) OVER(PARTITION BY SYMBOL ORDER BY DATE ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS Moving_Average
FROM abdeali9.stockmarketprice
WHERE SYMBOL=stock AND DATE BETWEEN start_date AND end_date;
END
//
DELIMITER ;
CALL moving_average_10('3MINDIA','2014-01-01','2014-01-30');


-- Calculate 52 Week High and 52 Week Low
DELIMITER //
CREATE PROCEDURE High_Low(stock1 text,stock2 text,start_date date,end_date date)
BEGIN
SELECT SYMBOL, DATE,
MAX(CLOSE) OVER(PARTITION BY SYMBOL ORDER BY DATE ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS 52Week_High
,MIN(CLOSE) OVER(PARTITION BY SYMBOL ORDER BY DATE ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS 52Week_Low
FROM abdeali9.stockmarketprice
WHERE SYMBOL in (stock1,stock2) AND
DATE BETWEEN start_date AND end_date;
END
//
DELIMITER ;

CALL High_Low('IIFLWAM','3MINDIA','2010-01-01','2015-01-01');
