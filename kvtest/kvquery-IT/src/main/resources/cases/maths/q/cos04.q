#cos function for standard degrees
select 
  cos(radians(0))    as cos0,
  cos(radians(30))   as cos30,
  cos(radians(45))   as cos45,
  cos(radians(60))   as cos60,
  cos(radians(90))   as cos90,
  cos(radians(180))  as cos180,
  cos(radians(270))  as cos270,
  cos(radians(360))  as cos360,
  cos(radians(-0.0)) as cos0neg,
  cos(radians(-30))  as cos30neg,
  cos(radians(-45))  as cos45neg,
  cos(radians(-60))  as cos60neg,
  cos(radians(-90))  as cos90neg,
  cos(radians(-180)) as cos180neg,
  cos(radians(-270)) as cos270neg,
  cos(radians(-360)) as cos360neg
from math_test where id=1

