//the result exceeded max value of TIMESTAMP type.
SELECT timestamp_add(tm9, '1 nanosecond')
FROM arithtest
WHERE id = 2