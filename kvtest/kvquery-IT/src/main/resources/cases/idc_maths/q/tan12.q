#tan function for standard degrees
select
  trunc(tan(radians(0)),7)    as tan0,
  trunc(tan(radians(30)),7)   as tan30,
  trunc(tan(radians(45)),7)   as tan45,
  trunc(tan(radians(60)),7)   as tan60,
  trunc(tan(radians(90)),7)   as tan90,
  trunc(tan(radians(180)),7)  as tan180,
  trunc(tan(radians(270)),7)  as tan270,
  trunc(tan(radians(360)),7)  as tan360,
  trunc(tan(radians(-0.0)),7) as tan0neg,
  trunc(tan(radians(-30)),7)  as tan30neg,
  trunc(tan(radians(-45)),7)  as tan45neg,
  trunc(tan(radians(-60)),7)  as tan60neg,
  trunc(tan(radians(-90)),7)  as tan90neg,
  trunc(tan(radians(-180)),7) as tan180neg,
  trunc(tan(radians(-270)),7) as tan270neg,
  trunc(tan(radians(-360)),7) as tan360neg
from functional_test where id=1

