#trunc of NaN and -0
select t.doc.dc as x, trunc(t.doc.dc,1) as truncx_1, trunc(t.doc.dc, t.doc.dc) as truncx_nan from math_test t where t.id in (8,9)

