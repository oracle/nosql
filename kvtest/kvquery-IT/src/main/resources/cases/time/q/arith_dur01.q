SELECT get_duration(cast(tm3 as long)) as DUR,
       (timestamp_add(0, get_duration(cast(tm3 as long))) = tm3) as RET
FROM arithtest
WHERE tm3 is not null
