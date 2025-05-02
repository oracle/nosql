select id, sqrt(t.iv)
from functional_test t
where sqrt(t.iv) > 0
order by id