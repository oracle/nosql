#ln of type limits

select
  ic, ln(ic) as lnic,
  lc, ln(lc) as lnlc,
  trunc(fc,2) as fc, trunc(ln(fc),2) as lnfc,
  dc, ln(dc) as lndc,
  nc, ln(nc) as lnnc
from math_test where id in(6,7) order by id

