DECLARE $dur1 STRING;  //"7 days 12 hours"
SELECT id
FROM arithtest 
WHERE timestamp_add(tm0, $dur1) > '2021-12-04'