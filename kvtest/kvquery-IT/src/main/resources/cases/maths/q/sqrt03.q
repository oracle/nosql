#sqrt of NaN and -0
select t.doc.dc as x, sqrt(t.doc.dc) as sqrtx from math_test t where t.id in (8,9)

