#TestCase for jsonCollection table

select
       cast(t.l3 as timestamp(3)) as l3,
       timestamp_floor(t.l3, 'day') as l3_to_day,
       timestamp_floor(t.l3, 'hour') as l3_to_hour,
       t.s6 as s6,
       timestamp_floor(t.s6) as s6_to_day1,
       timestamp_floor(t.s6, 'minute') as s6_to_minute,
       timestamp_floor(t.s6, 'second') as s6_to_second
from jsonCollection_test t
where id =1