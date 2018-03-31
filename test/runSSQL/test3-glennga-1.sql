-- Range partitioning is applied to the COMMENTS table. --
SELECT *
FROM COMMENTS
WHERE O_ORDERKEY % 1000 = 0;
