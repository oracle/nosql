SELECT timestamp_diff('2021-11-27', cast(tm0 as string)) as DIFF1,
       timestamp_diff(cast(tm0 as string), '2021-11-27') as DIFF2,
       timestamp_diff(86400000, 0) as DIFF3,       
       timestamp_diff('2020-03-01', $t.info.tm3) as DIFF4
FROM arithtest $t
WHERE id = 0