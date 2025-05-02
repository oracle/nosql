#power of NaN and -0
select t.doc.dc as x, power(t.doc.dc,1) as powerx from math_test t where t.id in (8,9)

