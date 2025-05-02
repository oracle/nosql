#TestCase for jsonCollection table

select
       cast(t.l3 as timestamp(3)) as l3,
       timestamp_bucket(t.l3, '2 weeks') as l3_to_2_weeks,
       timestamp_bucket(t.l3, '5 day') as l3_to_5_day,
       timestamp_bucket(t.l3, 'hour') as l3_to_hour,
       t.s6 as s6,
       timestamp_bucket(t.s6) as s6_to_day1,
       timestamp_bucket(t.s6, 'minute') as s6_to_minute,
       timestamp_bucket(t.s6, '30 second') as s6_to_30_second
from jsonCollection_test t
where id =1