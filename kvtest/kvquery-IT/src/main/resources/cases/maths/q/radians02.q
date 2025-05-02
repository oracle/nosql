#radians of null and missing field

select 
  t.ic, radians(t.ic),
  t.lc, radians(t.lc),
  t.fc, radians(t.fc),
  t.dc, radians(t.dc),
  t.nc, radians(t.nc),
  t.doc.ic, radians(t.doc.ic),
  t.doc.lc, radians(t.doc.lc),
  t.doc.fc, radians(t.doc.fc),
  t.doc.dc, radians(t.doc.dc),
  t.doc.nc, radians(t.doc.nc)
from math_test t where id=5

