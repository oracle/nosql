#cot function for scotdard radians
select 
  cot(0) as cot0,
  cot(pi()/6) as cot30,
  cot(pi()/4) as cot45,
  cot(pi()/3) as cot60,
  cot(pi()/2) as cot90,
  cot(pi()) as cot180,
  cot(3*pi()/2) as cot270,
  cot(2*pi()) as cot360,
  cot(-0.0) as cot0neg,
  cot(-pi()/6) as cot30neg,
  cot(-pi()/4) as cot45neg,
  cot(-pi()/3) as cot60neg,
  cot(-pi()/2) as cot90neg,
  cot(-pi()) as cot180neg,
  cot(-3*pi()/2) as cot270neg,
  cot(-2*pi()) as cot360neg
from math_test where id=1

