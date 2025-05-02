#round of NaN and -0
select t.doc.dc as x, round(t.doc.dc,1) as roundx_1, round(t.doc.dc, t.doc.dc) as roundx_nan from math_test t where t.id in (8,9)

