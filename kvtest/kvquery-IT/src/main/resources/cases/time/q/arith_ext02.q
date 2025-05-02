DECLARE $tm1 string;  //"2021-02-03T10:46:00"
SELECT timestamp_diff(tm0, $tm1) as DIFF
FROM arithtest 
WHERE id = 0