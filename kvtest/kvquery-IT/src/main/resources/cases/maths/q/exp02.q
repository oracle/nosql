#exp of null and missing field

select
  t.ic, exp(t.ic),
  t.lc, exp(t.lc),
  t.fc, exp(t.fc),
  t.dc, exp(t.dc),
  t.nc, exp(t.nc),
  t.doc.ic, exp(t.doc.ic),
  t.doc.lc, exp(t.doc.lc),
  t.doc.fc, exp(t.doc.fc),
  t.doc.dc, exp(t.doc.dc),
  t.doc.nc, exp(t.doc.nc)
from math_test t where id=5

