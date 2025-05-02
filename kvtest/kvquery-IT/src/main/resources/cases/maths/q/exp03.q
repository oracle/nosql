#exp of NaN and -0
select t.doc.dc as x, exp(t.doc.dc) as expx
from math_test t
where t.id in (8,9)

