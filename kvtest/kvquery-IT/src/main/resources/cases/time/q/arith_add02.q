SELECT tm9,
       timestamp_add(tm9, '12 hours') as P12H,
       timestamp_add(tm9, '-300 minutes') as SUB_P300MI,
       timestamp_add(tm9, '3600 seconds 500 milliseconds') as SUB_P3600S500MS,
       timestamp_add(tm9, '- 999 milliseconds 999999 nanoseconds') as SUB_P999MS999999NS,
       timestamp_add(tm9, '1 hour 1 minute 1 second 1 milliseconds 1 nanoseconds') as P1H1MI1S1MS1NS,
       timestamp_add(tm9, '- 1 hour 1 minute 1 second 1 milliseconds 1 nanoseconds') as SUB_P1H1MI1S1MS1NS
FROM arithtest
WHERE id = 0