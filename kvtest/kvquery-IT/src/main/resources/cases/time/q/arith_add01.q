SELECT tm0,
       timestamp_add(tm0, '1 year') as P1Y,
       timestamp_add(tm0, '-6 months') as SUB_P6M,
       timestamp_add(tm0, '30 days') as P30D,
       timestamp_add(tm0, '1 year 6 months 14 days') as P1Y6M14D,
       timestamp_add(tm0, '- 1 year 6 months 14 days') as SUB_P1Y6M14D
FROM arithtest
WHERE id = 0