#cos function for standard degrees
select
  trunc(cos(radians(0)),7)    as cos0,
  trunc(cos(radians(30)),7)   as cos30,
  trunc(cos(radians(45)),7)   as cos45,
  trunc(cos(radians(60)),7)   as cos60,
  trunc(cos(radians(90)),7)   as cos90,
  trunc(cos(radians(180)),7)  as cos180,
  trunc(cos(radians(270)),7)  as cos270,
  trunc(cos(radians(360)),7)  as cos360,
  trunc(cos(radians(-0.0)),7) as cos0neg,
  trunc(cos(radians(-30)),7)  as cos30neg,
  trunc(cos(radians(-45)),7)  as cos45neg,
  trunc(cos(radians(-60)),7)  as cos60neg,
  trunc(cos(radians(-90)),7)  as cos90neg,
  trunc(cos(radians(-180)),7) as cos180neg,
  trunc(cos(radians(-270)),7) as cos270neg,
  trunc(cos(radians(-360)),7) as cos360neg
from functional_test where id=1

