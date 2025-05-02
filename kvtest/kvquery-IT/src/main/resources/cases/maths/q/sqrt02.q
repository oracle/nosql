#sqrt of null and missing field

select 
  t.ic, sqrt(t.ic),
  t.lc, sqrt(t.lc),
  t.fc, sqrt(t.fc),
  t.dc, sqrt(t.dc),
  t.nc, sqrt(t.nc),
  t.doc.ic, sqrt(t.doc.ic),
  t.doc.lc, sqrt(t.doc.lc),
  t.doc.fc, sqrt(t.doc.fc),
  t.doc.dc, sqrt(t.doc.dc),
  t.doc.nc, sqrt(t.doc.nc)
from math_test t where id=5

