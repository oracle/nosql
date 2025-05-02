#ln of NaN and -0
select t.doc.dc as x, ln(t.doc.dc) as lnx from math_test t where t.id in (8,9)

