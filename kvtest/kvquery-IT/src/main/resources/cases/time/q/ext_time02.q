declare $tm2 string;
SELECT *
FROM bar
WHERE tm > CAST($tm2 AS TIMESTAMP(1))
