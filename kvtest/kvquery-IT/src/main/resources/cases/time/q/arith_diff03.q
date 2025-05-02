SELECT timestamp_diff(current_time(), tm0) as DIFF1,
       timestamp_diff(tm3, current_time()) as DIFF2,
       timestamp_diff($t.info.tm0, current_time()) as DIFF3,
       timestamp_diff(current_time(), $t.info.tm0) as DIFF4
FROM arithtest $t
WHERE id = 1