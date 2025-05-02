#lgten of type limits

select
  ic, log10(ic) as lgtenic,
  lc, log10(lc) as lgtenlc,
  trunc(fc, 2) as fc, trunc(log10(fc),2) as lgtenfc,
  dc, log10(dc) as lgtendc,
  nc, log10(nc) as lgtennc
from math_test where id in(6,7) order by id

