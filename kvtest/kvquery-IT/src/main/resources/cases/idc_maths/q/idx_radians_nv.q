select id, radians(t.nv)
from functional_test t
where radians(t.nv) > 0
order by id