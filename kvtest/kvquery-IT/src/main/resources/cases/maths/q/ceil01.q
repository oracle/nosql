#ceil of positive numbers
select
  t.ic, ceil(t.ic) as ceilic,
  t.lc, ceil(t.lc) as ceillc,
  trunc(t.fc,2) as fc, trunc(ceil(t.fc),2) as ceilfc,
  t.dc, ceil(t.dc) as ceildc,
  t.nc, ceil(t.nc) as ceilnc,
  t.doc.ic as icjson, ceil(t.doc.ic) as ceilicjson,
  t.doc.lc as lcjson, ceil(t.doc.lc) as ceillcsjon,
  t.doc.fc as fcjson, ceil(t.doc.fc) as ceilfcsjon,
  t.doc.dc as dcjson, ceil(t.doc.dc) as ceildcsjon,
  t.doc.nc as ncjson, ceil(t.doc.nc) as ceilncsjon
from math_test t where t.id=1

