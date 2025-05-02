SELECT timestamp_diff(tm0, tm3) as DIFF_T0_T3,
       get_duration(timestamp_diff(tm0, tm3)) as DURATION_T0_T3,
       tm0 = timestamp_add(tm3, get_duration(timestamp_diff(tm0, tm3))) as RET_T0, 
       timestamp_diff(tm3, tm0) as DIFF_T3_T0,
       get_duration(timestamp_diff(tm3, tm0)) as DURATION_T3_T0,
       tm3 = timestamp_add(tm0, get_duration(timestamp_diff(tm3, tm0))) as RET_T3
FROM arithtest 
WHERE id = 2