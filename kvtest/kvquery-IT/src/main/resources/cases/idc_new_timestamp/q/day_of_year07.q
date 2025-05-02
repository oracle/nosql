#TestCase for jsonCollection table

select
       cast(t.l3 as timestamp(3)) as l3,
       day_of_year(t.l3),
       t.s6 as s6,
       day_of_year(t.s6)
from jsonCollection_test t
where id =1