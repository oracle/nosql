#tan function for standard degrees
select 
  tan(radians(0))    as tan0,
  tan(radians(30))   as tan30,
  tan(radians(45))   as tan45,
  tan(radians(60))   as tan60,
  tan(radians(90))   as tan90,
  tan(radians(180))  as tan180,
  tan(radians(270))  as tan270,
  tan(radians(360))  as tan360,
  tan(radians(-0.0)) as tan0neg,
  tan(radians(-30))  as tan30neg,
  tan(radians(-45))  as tan45neg,
  tan(radians(-60))  as tan60neg,
  tan(radians(-90))  as tan90neg,
  tan(radians(-180)) as tan180neg,
  tan(radians(-270)) as tan270neg,
  tan(radians(-360)) as tan360neg
from math_test where id=1

