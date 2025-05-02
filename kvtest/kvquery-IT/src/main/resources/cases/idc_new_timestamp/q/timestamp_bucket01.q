#Test cases for valid timestamp and unit
select
       cast(t.doc.l3 as timestamp(3)) as jl3,
       timestamp_bucket(t.doc.l3, '2 weeks') as jl3_to_2_weeks,
       timestamp_bucket(t.doc.l3, '5 days') as jl3_to_5_day,
       timestamp_bucket(t.doc.l3, '5 hours') as jl3_to_5_hour,
       t.doc.s6 as s6,
       timestamp_bucket(t.doc.s6) as s6_to_day1,
       timestamp_bucket(t.doc.s6, '10 minutes') as s6_to_10_minute,
       timestamp_bucket(t.doc.s6, '30 seconds') as s6_to_30_second
from roundFunc t
where id =0
