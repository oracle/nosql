SELECT timestamp_add(tm0, '1 day') as T1,
       timestamp_add(current_time(), duration) as T2,
       timestamp_add($t.info.tm3, '1 day') as T3,
       timestamp_add(current_time(), $t.info.dur) as T4
FROM arithtest $t
WHERE id = 1