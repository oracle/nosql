#cos of NaN and -0
select t.doc.dc as x, cos(t.doc.dc) as cosx
from math_test t
where t.id in (8,9)

