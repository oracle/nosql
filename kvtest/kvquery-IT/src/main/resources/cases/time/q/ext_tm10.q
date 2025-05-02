declare $tm4 string; // "2021-02-03T10:45:00.23406"
SELECT *
FROM bar
WHERE tm >= $tm4
