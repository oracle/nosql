#degrees of NaN and -0
select t.doc.dc as x, degrees(t.doc.dc) as degreesx
from math_test t where
t.id in (8,9)

