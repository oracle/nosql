SELECT tm3, tm9,
       timestamp_add(tm3, '1 day') as T3_P1D,
       timestamp_add(tm9, '1 day') as T9_P1D,
       timestamp_add(tm3, '1 millisecond') as T3_P1MS,
       timestamp_add(tm9, '1 nanosecond') as T9_P1NS
FROM arithtest
WHERE id = 0