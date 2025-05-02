select t0,
       timestamp_bucket(t0, '9 SECOND') as t0_9secs,
       s9,
       timestamp_bucket(s9, '5 MINUTEs') as s9_5mins,
       t3,
       timestamp_bucket(t3, '2 hour') as t3_2hours,
       timestamp_bucket(t3) as t3_day,
       t.doc.s6 as s6,
       timestamp_bucket(t.doc.s6, '5 Days') as s6_5days,
       timestamp_bucket(t.doc.s6, '2 weeks') as s6_2weeks
from roundtest t
where id = 0
