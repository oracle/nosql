#log10 of NaN and -0
select t.doc.dc as x, log10(t.doc.dc) as log10x from math_test t where t.id in (8,9)

