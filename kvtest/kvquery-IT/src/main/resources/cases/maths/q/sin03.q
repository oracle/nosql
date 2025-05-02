#sin of NaN and -0
select t.doc.dc as x, sin(t.doc.dc) as sinx from math_test t where t.id in (8,9)

