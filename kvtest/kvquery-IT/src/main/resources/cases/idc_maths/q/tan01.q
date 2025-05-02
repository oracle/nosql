#tan function for standard radians
select
  trunc(tan(0),7) as tan0,
  trunc(tan(pi()/6),7) as tan30,
  trunc(tan(pi()/4),7) as tan45,
  trunc(tan(pi()/3),7) as tan60,
  trunc(tan(pi()/2),7) as tan90,
  trunc(tan(pi()),7) as tan180,
  trunc(tan(3*pi()/2),7) as tan270,
  trunc(tan(2*pi()),7) as tan360,
  trunc(tan(-0.0),7) as tan0neg,
  trunc(tan(-pi()/6),7) as tan30neg,
  trunc(tan(-pi()/4),7) as tan45neg,
  trunc(tan(-pi()/3),7) as tan60neg,
  trunc(tan(-pi()/2),7) as tan90neg,
  trunc(tan(-pi()),7) as tan180neg,
  trunc(tan(-3*pi()/2),7) as tan270neg,
  trunc(tan(-2*pi()),7) as tan360neg
from functional_test where id=1