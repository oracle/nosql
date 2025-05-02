#tan of NaN and mistang field

select 
  t.ic, tan(t.ic),
  t.lc, tan(t.lc),
  t.fc, tan(t.fc),
  t.dc, tan(t.dc),
  t.nc, tan(t.nc),
  t.doc.ic, tan(t.doc.ic),
  t.doc.lc, tan(t.doc.lc),
  t.doc.fc, tan(t.doc.fc),
  t.doc.dc, tan(t.doc.dc),
  t.doc.nc, tan(t.doc.nc)
from math_test t where id=5

