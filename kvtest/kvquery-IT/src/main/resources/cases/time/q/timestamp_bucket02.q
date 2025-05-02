select timestamp_bucket(s9, '2 weeks', '2021-01-01') as b_2weeks,
       timestamp_bucket(s9, '5 days', '2021-02-01') as b_5days,
       timestamp_bucket(s9, '6 hours', '2021-02-28T01') as b_6hours,
       timestamp_bucket(s9, '7 minutes', '2021-03-01T23:30:00.999') as b_7mins,
       timestamp_bucket(s9, '18 seconds', '2021-02-01T00:00:30.000000001') as b_18secs
from roundtest t
where id = 0
