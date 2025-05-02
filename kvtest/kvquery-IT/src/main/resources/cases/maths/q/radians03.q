#radians of NaN and -0
select t.doc.dc as x, radians(t.doc.dc) as radiansx from math_test t where t.id in (8,9)

