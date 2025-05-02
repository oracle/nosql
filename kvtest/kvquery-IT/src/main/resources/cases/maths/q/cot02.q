#cot of NaN and miscotg field

select 
  t.ic, cot(t.ic),
  t.lc, cot(t.lc),
  t.fc, cot(t.fc),
  t.dc, cot(t.dc),
  t.nc, cot(t.nc),
  t.doc.ic, cot(t.doc.ic),
  t.doc.lc, cot(t.doc.lc),
  t.doc.fc, cot(t.doc.fc),
  t.doc.dc, cot(t.doc.dc),
  t.doc.nc, cot(t.doc.nc)
from math_test t where id=5

