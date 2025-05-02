#asin test
select t.ic, asin(t.ic) as asinic,
    t.lc, asin(t.lc) as asinlc,
    trunc(t.fc,2) as fc, trunc(asin(t.fc),2) as asinfc,
    t.dc, asin(t.dc) as asindc,
    t.nc, asin(t.nc) as asinnc,
    t.doc.fc as jsonfc, asin(t.doc.fc) as asinjsonfc,
    t.doc.dc as jsondc, asin(t.doc.dc) as asinjsondc,
    t.doc.nc as jsonnc, asin(t.doc.nc) as asinjsonnc
from math_test t where t.id=10
