SELECT timestamp_add(tm0, duration) as T0,
       timestamp_add(tm3, duration) as T3, 
       timestamp_add(tm9, duration) as T9,
       timestamp_add($t.info.tm3, duration) as JT3,
       timestamp_add($t.info.tm6, duration) as JT6
FROM arithtest $t
WHERE id = 0