#floor of max values
select 
  t.ic, floor(t.ic) as flooric,
  t.lc, floor(t.lc) as floorlc,
  trunc(t.fc,2) as fc, trunc(floor(t.fc),2) as floorfc,
  t.dc, floor(t.dc) as floordc,
  t.nc, floor(t.nc) as floornc,
  t.doc.ic as jsonic, floor(t.doc.ic) as flooricjson,
  t.doc.lc as jsonlc, floor(t.doc.lc) as floorlcjson,
  t.doc.fc as jsonfc, floor(t.doc.fc) as floorfcjson,
  t.doc.nc as jsondc, floor(t.doc.dc) as floordcjson,
  t.doc.fc as jsonnc, floor(t.doc.nc) as floorncjson
from math_test t where t.id=6

