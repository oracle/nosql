select id, sin(t.fv)
from functional_test t
where sin(t.fv) > 0
order by id