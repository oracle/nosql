#sign of NaN and -0
select t.doc.dc as x, sign(t.doc.dc) as signx from math_test t where t.id in (8,9)

