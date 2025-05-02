#power of null and missing field

select 
  t.ic, power(t.ic,1),
  t.lc, power(t.lc,null),
  t.fc, power(t.fc,2),
  t.dc, power(t.dc,3),
  t.nc, power(t.nc,0),
  t.doc.ic, power(t.doc.ic,-1),
  t.doc.lc, power(t.doc.lc,-0.5),
  t.doc.fc, power(t.doc.fc,0.5),
  t.doc.dc, power(t.doc.dc,1/3.0),
  t.doc.nc, power(t.doc.nc,null)
from math_test t where id=5

