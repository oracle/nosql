#TestCase for jsonCollection table

select
       cast(t.l3 as timestamp(3)) as l3,
       to_last_day_of_month(t.l3),
       t.s6 as s6,
       to_last_day_of_month(t.s6)
from jsonCollection_test t
where id =1