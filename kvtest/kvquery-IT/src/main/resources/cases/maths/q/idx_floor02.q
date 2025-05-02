select id, floor(t.doc.dc)
from math_test t
where floor(t.doc.dc) > 0
order by id
