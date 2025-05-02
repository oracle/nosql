#TestCase for jsonCollection table

select
       cast(t.l3 as timestamp(3)) as l3,
       quarter(t.l3),
       t.s6 as s6,
       quarter(t.s6)
from jsonCollection_test t
where id =1