#Test cases for valid timestamp and unit
select t0,
       timestamp_ceil(t0, 'year') as t0_to_year,
       timestamp_ceil(t0, 'iyear') as t0_to_iyear,
       t3,
       timestamp_ceil(t3, 'QUARTER') as t0_to_quarter,
       timestamp_ceil(t3, 'Month') as t3_to_month,
       cast(l3 as timestamp(3)) as l3,
       timestamp_ceil(l3, 'weeK') as l3_to_week,
       timestamp_ceil(l3, 'iweeK') as l3_to_iweek,
       cast(t.doc.l3 as timestamp(3)) as jl3,
       timestamp_ceil(t.doc.l3, 'day') as jl3_to_day,
       timestamp_ceil(t.doc.l3, 'hour') as jl3_to_hour,
       t.doc.s6 as s6,
       timestamp_ceil(t.doc.s6) as s6_to_day1,
       timestamp_ceil(t.doc.s6, 'minute') as s6_to_minute,
       timestamp_ceil(t.doc.s6, 'second') as s6_to_second
from roundFunc t
where id =0