#Test cases for valid timestamp and unit
select
       cast(t.doc.l3 as timestamp(3)) as jl3,
       timestamp_floor(t.doc.l3, 'day') as jl3_to_day,
       timestamp_floor(t.doc.l3, 'hour') as jl3_to_hour,
       t.doc.s6 as s6,
       timestamp_floor(t.doc.s6) as s6_to_day1,
       timestamp_floor(t.doc.s6, 'minute') as s6_to_minute,
       timestamp_floor(t.doc.s6, 'second') as s6_to_second
from roundFunc t
where id =0