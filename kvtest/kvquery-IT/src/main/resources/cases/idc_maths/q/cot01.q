#cot function for scotdard radians
select
  trunc(cot(0),7) as cot0,
  trunc(cot(pi()/6),7) as cot30,
  trunc(cot(pi()/4),7) as cot45,
  trunc(cot(pi()/3),7) as cot60,
  trunc(cot(pi()/2),7) as cot90,
  trunc(cot(pi()),7) as cot180,
  trunc(cot(3*pi()/2),7) as cot270,
  trunc(cot(2*pi()),7) as cot360,
  trunc(cot(-0.0),7) as cot0neg,
  trunc(cot(-pi()/6),7) as cot30neg,
  trunc(cot(-pi()/4),7) as cot45neg,
  trunc(cot(-pi()/3),7) as cot60neg,
  trunc(cot(-pi()/2),7) as cot90neg,
  trunc(cot(-pi()),7) as cot180neg,
  trunc(cot(-3*pi()/2),7) as cot270neg,
  trunc(cot(-2*pi()),7) as cot360neg
from functional_test where id=1

