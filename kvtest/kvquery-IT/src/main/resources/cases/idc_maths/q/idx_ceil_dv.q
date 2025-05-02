select id, ceil(t.dv)
from functional_test t
where ceil(t.dv) > 0
order by id