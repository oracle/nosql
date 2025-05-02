#sin of NaN and missing field

select 
  t.ic, sin(t.ic),
  t.lc, sin(t.lc),
  t.fc, sin(t.fc),
  t.dc, sin(t.dc),
  t.nc, sin(t.nc),
  t.doc.ic, sin(t.doc.ic),
  t.doc.lc, sin(t.doc.lc),
  t.doc.fc, sin(t.doc.fc),
  t.doc.dc, sin(t.doc.dc),
  t.doc.nc, sin(t.doc.nc)
from math_test t where id=5

