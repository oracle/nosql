#tan function for standard radians
select 
  tan(0) as tan0,
  tan(pi()/6) as tan30,
  tan(pi()/4) as tan45,
  tan(pi()/3) as tan60,
  tan(pi()/2) as tan90,
  tan(pi()) as tan180,
  tan(3*pi()/2) as tan270,
  tan(2*pi()) as tan360,
  tan(-0.0) as tan0neg,
  tan(-pi()/6) as tan30neg,
  tan(-pi()/4) as tan45neg,
  tan(-pi()/3) as tan60neg,
  tan(-pi()/2) as tan90neg,
  tan(-pi()) as tan180neg,
  tan(-3*pi()/2) as tan270neg,
  tan(-2*pi()) as tan360neg
from math_test where id=1

