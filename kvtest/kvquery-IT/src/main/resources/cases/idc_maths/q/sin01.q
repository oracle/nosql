#sin function for standard radians
select
  trunc(sin(0),7) as sin0,
  trunc(sin(pi()/6),7) as sin30,
  trunc(sin(pi()/4),7) as sin45,
  trunc(sin(pi()/3),7) as sin60,
  trunc(sin(pi()/2),7) as sin90,
  trunc(sin(pi()),7) as sin180,
  trunc(sin(3*pi()/2),7) as sin270,
  trunc(sin(2*pi()),7) as sin360,
  trunc(sin(-0.0),7) as sin0neg,
  trunc(sin(-pi()/6),7) as sin30neg,
  trunc(sin(-pi()/4),7) as sin45neg,
  trunc(sin(-pi()/3),7) as sin60neg,
  trunc(sin(-pi()/2),7) as sin90neg,
  trunc(sin(-pi()),7) as sin180neg,
  trunc(sin(-3*pi()/2),7) as sin270neg,
  trunc(sin(-2*pi()),7) as sin360neg
from functional_test where id=1