#cot function for standard degrees
select
trunc(cot(radians(0)),7)   as cot0,
trunc(cot(radians(30)),7)   as cot30,
trunc(cot(radians(45)),7)   as cot45,
trunc(cot(radians(60)),7)   as cot60,
trunc(cot(radians(90)),7)   as cot90,
trunc(cot(radians(180)),7)  as cot180,
trunc(cot(radians(270)),7)  as cot270,
trunc(cot(radians(360)),7)  as cot360,
trunc(cot(radians(-0.0)),7) as cot0neg,
trunc(cot(radians(-30)),7)  as cot30neg,
trunc(cot(radians(-45)),7)  as cot45neg,
trunc(cot(radians(-60)),7)  as cot60neg,
trunc(cot(radians(-90)),7)  as cot90neg,
trunc(cot(radians(-180)),7) as cot180neg,
trunc(cot(radians(-270)),7) as cot270neg,
trunc(cot(radians(-360)),7) as cot360neg
from functional_test where id=1

