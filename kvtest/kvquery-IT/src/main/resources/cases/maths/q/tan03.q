#tan of NaN and -0
select t.doc.dc as x, tan(t.doc.dc) as tanx from math_test t where t.id in (8,9)

