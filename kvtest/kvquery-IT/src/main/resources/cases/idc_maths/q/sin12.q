#sin function for standard degrees
select
  trunc(sin(radians(0)),7)    as sin0,
  trunc(sin(radians(30)),7)   as sin30,
  trunc(sin(radians(45)),7)   as sin45,
  trunc(sin(radians(60)),7)   as sin60,
  trunc(sin(radians(90)),7)   as sin90,
  trunc(sin(radians(180)),7)  as sin180,
  trunc(sin(radians(270)),7)  as sin270,
  trunc(sin(radians(360)),7)  as sin360,
  trunc(sin(radians(-0.0)),7) as sin0neg,
  trunc(sin(radians(-30)),7)  as sin30neg,
  trunc(sin(radians(-45)),7)  as sin45neg,
  trunc(sin(radians(-60)),7)  as sin60neg,
  trunc(sin(radians(-90)),7)  as sin90neg,
  trunc(sin(radians(-180)),7) as sin180neg,
  trunc(sin(radians(-270)),7) as sin270neg,
  trunc(sin(radians(-360)),7) as sin360neg
from functional_test where id=1

