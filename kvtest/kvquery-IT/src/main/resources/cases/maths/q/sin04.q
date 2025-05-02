#sin function for standard degrees
select 
  sin(radians(0))    as sin0,
  sin(radians(30))   as sin30,
  sin(radians(45))   as sin45,
  sin(radians(60))   as sin60,
  sin(radians(90))   as sin90,
  sin(radians(180))  as sin180,
  sin(radians(270))  as sin270,
  sin(radians(360))  as sin360,
  sin(radians(-0.0)) as sin0neg,
  sin(radians(-30))  as sin30neg,
  sin(radians(-45))  as sin45neg,
  sin(radians(-60))  as sin60neg,
  sin(radians(-90))  as sin90neg,
  sin(radians(-180)) as sin180neg,
  sin(radians(-270)) as sin270neg,
  sin(radians(-360)) as sin360neg
from math_test where id=1

