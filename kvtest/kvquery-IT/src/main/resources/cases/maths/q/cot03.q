#cot of NaN and -0
select t.doc.dc as x, cot(t.doc.dc) as cotx
from math_test t
where t.id in (8,9)

