#ceil of min values
select 
  t.ic, ceil(t.ic) as ceilic,
  t.lc, ceil(t.lc) as ceillc,
  trunc(t.fc,2) as fc, trunc(ceil(t.fc),2) as ceilfc,
  t.dc, ceil(t.dc) as ceildc,
  t.nc, ceil(t.nc) as ceilnc,
  t.doc.ic as jsonic, ceil(t.doc.ic) as ceilicjson,
  t.doc.lc as jsonlc, ceil(t.doc.lc) as ceillcjson,
  t.doc.fc as jsonfc, ceil(t.doc.fc) as ceilfcjson,
  t.doc.nc as jsondc, ceil(t.doc.dc) as ceildcjson,
  t.doc.fc as jsonnc, ceil(t.doc.nc) as ceilncjson
from math_test t where t.id=7

