#sin function for standard radians
select 
  sin(0) as sin0,
  sin(pi()/6) as sin30,
  sin(pi()/4) as sin45,
  sin(pi()/3) as sin60,
  sin(pi()/2) as sin90,
  sin(pi()) as sin180,
  sin(3*pi()/2) as sin270,
  sin(2*pi()) as sin360,
  sin(-0.0) as sin0neg,
  sin(-pi()/6) as sin30neg,
  sin(-pi()/4) as sin45neg,
  sin(-pi()/3) as sin60neg,
  sin(-pi()/2) as sin90neg,
  sin(-pi()) as sin180neg,
  sin(-3*pi()/2) as sin270neg,
  sin(-2*pi()) as sin360neg
from math_test where id=1

