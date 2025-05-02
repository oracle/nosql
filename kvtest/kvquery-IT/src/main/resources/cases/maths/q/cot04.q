#cot function for scotdard degrees
select 
  cot(radians(0))    as cot0,
  cot(radians(30))   as cot30,
  cot(radians(45))   as cot45,
  cot(radians(60))   as cot60,
  cot(radians(90))   as cot90,
  cot(radians(180))  as cot180,
  cot(radians(270))  as cot270,
  cot(radians(360))  as cot360,
  cot(radians(-0.0)) as cot0neg,
  cot(radians(-30))  as cot30neg,
  cot(radians(-45))  as cot45neg,
  cot(radians(-60))  as cot60neg,
  cot(radians(-90))  as cot90neg,
  cot(radians(-180)) as cot180neg,
  cot(radians(-270)) as cot270neg,
  cot(radians(-360)) as cot360neg
from math_test where id=1

