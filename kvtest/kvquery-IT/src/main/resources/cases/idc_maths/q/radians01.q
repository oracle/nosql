#degrees to radians of standard angles
select
  0   as degrees0, trunc(radians(0),7) as radians0,
  1   as degrees1, trunc(radians(1),7) as radians1,
  30  as degrees30, trunc(radians(30),7) as radians30,
  45  as degrees45, trunc(radians(45),7) as radians45,
  60  as degrees60, trunc(radians(60),7) as radians60,
  90  as degrees90, trunc(radians(90),7) as radians90,
  120 as degrees120, trunc(radians(120),7) as radians120,
  135 as degrees135, trunc(radians(135),7) as radians135,
  180 as degrees180, trunc(radians(180),7) as radians180,
  270 as degrees270, trunc(radians(270),7) as radians270,
  360 as degrees360, trunc(radians(360),7) as radians360,
  -0   as degreesNeg0, trunc(radians(-0),7) as radiansNeg0,
  -1   as degreesNeg1, trunc(radians(-1),7) as radiansNeg1,
  -30  as degreesNeg30, trunc(radians(-30),7) as radiansNeg30,
  -45  as degreesNeg45, trunc(radians(-45),7) as radiansNeg45,
  -60  as degreesNeg60, trunc(radians(-60),7) as radiansNeg60,
  -90  as degreesNeg90, trunc(radians(-90),7) as radiansNeg90,
  -120 as degreesNeg120, trunc(radians(-120),7) as radiansNeg120,
  -135 as degreesNeg135, trunc(radians(-135),7) as radiansNeg135,
  -180 as degreesNeg180, trunc(radians(-180),7) as radiansNeg180,
  -270 as degreesNeg270, trunc(radians(-270),7) as radiansNeg270,
  -360 as degreesNeg360, trunc(radians(-360),7) as radiansNeg360
from functional_test where id=1


