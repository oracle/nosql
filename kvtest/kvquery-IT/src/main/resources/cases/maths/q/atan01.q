#atan test
select t.ic, atan(t.ic) as atanic,
    t.lc, atan(t.lc) as atanlc,
    trunc(t.fc,2) as fc, trunc(atan(t.fc),2) as atanfc,
    t.dc, atan(t.dc) as atandc,
    t.nc, atan(t.nc) as atannc,
    t.doc.fc as jsonfc, atan(t.doc.fc) as atanjsonfc,
    t.doc.dc as jsondc, atan(t.doc.dc) as atanjsondc,
    t.doc.nc as jsonnc, atan(t.doc.nc) as atanjsonnc
from math_test t where t.id=10
