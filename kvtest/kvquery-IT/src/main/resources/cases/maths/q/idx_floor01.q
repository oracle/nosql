select id, floor(t.dc)
from math_test t
where floor(t.dc) > 0
order by id
