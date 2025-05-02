#degree of null and missing field

select 
  t.ic, degrees(t.ic),
  t.lc, degrees(t.lc),
  t.fc, degrees(t.fc),
  t.dc, degrees(t.dc),
  t.nc, degrees(t.nc),
  t.doc.ic, degrees(t.doc.ic),
  t.doc.lc, degrees(t.doc.lc),
  t.doc.fc, degrees(t.doc.fc),
  t.doc.dc, degrees(t.doc.dc),
  t.doc.nc, degrees(t.doc.nc)
from math_test t where id=5

