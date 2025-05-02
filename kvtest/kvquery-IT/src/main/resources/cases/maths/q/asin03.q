#asin of trignometic angles
select asin(sin(0)) = 0,
  trunc(asin(sin(pi()/6)),4) = trunc(pi()/6,4),
  trunc(asin(sin(pi()/4)),4) = trunc(pi()/4,4),
  trunc(asin(sin(pi()/3)),4) = trunc(pi()/3,4),
  trunc(asin(sin(pi()/2)),4) = trunc(pi()/2,4),
  trunc(asin(sin(-pi()/6)),4) = trunc(-pi()/6,4),
  trunc(asin(sin(-pi()/4)),4) = trunc(-pi()/4,4),
  trunc(asin(sin(-pi()/3)),4) = trunc(-pi()/3,4),
  trunc(asin(sin(-pi()/2)),4) = trunc(-pi()/2,4)
from math_test where id=1

