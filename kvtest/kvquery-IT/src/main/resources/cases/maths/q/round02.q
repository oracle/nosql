#round of null and missing field

select 
  t.ic, round(t.ic,1),
  t.lc, round(t.lc,null),
  t.fc, round(t.fc,2),
  t.dc, round(t.dc,3),
  t.nc, round(t.nc,0),
  t.doc.ic, round(t.doc.ic,-1),
  t.doc.lc, round(t.doc.lc,-0.5),
  t.doc.fc, round(t.doc.fc,0.5),
  t.doc.dc, round(t.doc.dc,1/3.0),
  t.doc.nc, round(t.doc.nc,null)
from math_test t where id=5

