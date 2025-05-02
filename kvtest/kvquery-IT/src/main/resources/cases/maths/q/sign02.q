#sign of null and missing field

select 
  t.ic, sign(t.ic),
  t.lc, sign(t.lc),
  t.fc, sign(t.fc),
  t.dc, sign(t.dc),
  t.nc, sign(t.nc),
  t.doc.ic, sign(t.doc.ic),
  t.doc.lc, sign(t.doc.lc),
  t.doc.fc, sign(t.doc.fc),
  t.doc.dc, sign(t.doc.dc),
  t.doc.nc, sign(t.doc.nc)
from math_test t where id=5

