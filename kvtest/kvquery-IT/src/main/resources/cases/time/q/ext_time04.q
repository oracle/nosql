declare $tm3 string;
SELECT *
FROM bar
WHERE tm > CAST($tm3 AS TIMESTAMP(5))
