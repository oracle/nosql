#atan of trignometic angles
select atan(tan(0)) = 0,
  trunc(atan(tan(pi()/6)),4) = trunc(pi()/6,4),
  trunc(atan(tan(pi()/4)),4) = trunc(pi()/4,4),
  trunc(atan(tan(pi()/3)),4) = trunc(pi()/3,4),
  trunc(atan(tan(pi()/2)),4) = trunc(pi()/2,4),
  trunc(atan(tan(-pi()/6)),4) = trunc(-pi()/6,4),
  trunc(atan(tan(-pi()/4)),4) = trunc(-pi()/4,4),
  trunc(atan(tan(-pi()/3)),4) = trunc(-pi()/3,4),
  trunc(atan(tan(-pi()/2)),4) = trunc(-pi()/2,4)
from math_test where id=1

