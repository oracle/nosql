#floor of positive numbers
select
  t.ic, floor(t.ic) as flooric,
  t.lc, floor(t.lc) as floorlc,
  trunc(t.fc,2) as fc, trunc(floor(t.fc),2) as floorfc,
  t.dc, floor(t.dc) as floordc,
  t.nc, floor(t.nc) as floornc,
  t.doc.ic as icjson, floor(t.doc.ic) as flooricjson,
  t.doc.lc as lcjson, floor(t.doc.lc) as floorlcsjon,
  t.doc.fc as fcjson, floor(t.doc.fc) as floorfcsjon,
  t.doc.dc as dcjson, floor(t.doc.dc) as floordcsjon,
  t.doc.nc as ncjson, floor(t.doc.nc) as floorncsjon
from math_test t where t.id=1

