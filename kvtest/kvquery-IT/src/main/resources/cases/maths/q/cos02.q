#cos of NaN and miscosg field

select 
  t.ic, cos(t.ic),
  t.lc, cos(t.lc),
  t.fc, cos(t.fc),
  t.dc, cos(t.dc),
  t.nc, cos(t.nc),
  t.doc.ic, cos(t.doc.ic),
  t.doc.lc, cos(t.doc.lc),
  t.doc.fc, cos(t.doc.fc),
  t.doc.dc, cos(t.doc.dc),
  t.doc.nc, cos(t.doc.nc)
from math_test t where id=5

