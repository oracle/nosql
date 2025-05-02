#ln of null and missing field

select
  t.ic, ln(t.ic),
  t.lc, ln(t.lc),
  t.fc, ln(t.fc),
  t.dc, ln(t.dc),
  t.nc, ln(t.nc),
  t.doc.ic, ln(t.doc.ic),
  t.doc.lc, ln(t.doc.lc),
  t.doc.fc, ln(t.doc.fc),
  t.doc.dc, ln(t.doc.dc),
  t.doc.nc, ln(t.doc.nc)
from math_test t where id=5

