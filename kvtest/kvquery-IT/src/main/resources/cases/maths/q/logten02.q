#log10 of null and missing field

select
  t.ic, log10(t.ic),
  t.lc, log10(t.lc),
  t.fc, log10(t.fc),
  t.dc, log10(t.dc),
  t.nc, log10(t.nc),
  t.doc.ic, log10(t.doc.ic),
  t.doc.lc, log10(t.doc.lc),
  t.doc.fc, log10(t.doc.fc),
  t.doc.dc, log10(t.doc.dc),
  t.doc.nc, log10(t.doc.nc)
from math_test t where id=5

