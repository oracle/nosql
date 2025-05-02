//the result is less than min value of TIMESTAMP type.
SELECT timestamp_add(tm0, '- 1 nanosecond')
FROM arithtest
WHERE id = 2