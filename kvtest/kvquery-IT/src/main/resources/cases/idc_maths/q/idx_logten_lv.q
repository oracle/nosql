select id, log10(t.lv)
from functional_test t
where log10(t.lv) > 0
order by id