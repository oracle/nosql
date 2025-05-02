select id, atan2(t.dv,3)
from functional_test t
where atan2(t.dv,3) > 0
order by id