#trunc of null and missing field

select 
  t.ic, trunc(t.ic,1),
  t.lc, trunc(t.lc,null),
  t.fc, trunc(t.fc,2),
  t.dc, trunc(t.dc,3),
  t.nc, trunc(t.nc,0),
  t.doc.ic, trunc(t.doc.ic,-1),
  t.doc.lc, trunc(t.doc.lc,-0.5),
  t.doc.fc, trunc(t.doc.fc,0.5),
  t.doc.dc, trunc(t.doc.dc,1/3.0),
  t.doc.nc, trunc(t.doc.nc,null)
from math_test t where id=5

