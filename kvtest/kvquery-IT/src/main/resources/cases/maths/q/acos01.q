#acos test
select
  t.ic, acos(t.ic) as acosic,
  t.lc, acos(t.lc) as acoslc,
  trunc(t.fc,2) as fc, trunc(acos(t.fc),2) as acosfc,
  t.dc, acos(t.dc) as acosdc,
  t.nc, acos(t.nc) as acosnc,
  t.doc.fc as jsonfc, acos(t.doc.fc) as acosjsonfc,
  t.doc.dc as jsondc, acos(t.doc.dc) as acosjsondc,
  t.doc.nc as jsonnc, acos(t.doc.nc) as acosjsonnc
from math_test t where t.id=10

