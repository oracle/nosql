DECLARE $dur2 long;   //3600999
SELECT timestamp_add(tm0, get_duration($dur2)) as T0
FROM arithtest 
WHERE id = 0