-- Hash partitioning is applied to the COMMENTS table. --
SELECT *
FROM ORDERS
WHERE O_ORDERKEY < 1000;