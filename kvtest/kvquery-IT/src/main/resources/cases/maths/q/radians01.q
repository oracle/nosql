#degrees to radians of standard angles
select
  0   as degrees0, radians(0) as radians0,
  1   as degrees1, radians(1) as radians1,
  30  as degrees30, radians(30) as radians30,
  45  as degrees45, radians(45) as radians45,
  60  as degrees60, radians(60) as radians60,
  90  as degrees90, radians(90) as radians90,
  120 as degrees120, radians(120) as radians120,
  135 as degrees135, radians(135) as radians135,
  180 as degrees180, radians(180) as radians180,
  270 as degrees270, radians(270) as radians270,
  360 as degrees360, radians(360) as radians360,
  -0   as degreesNeg0, radians(0) as radiansNeg0,
  -1   as degreesNeg1, radians(1) as radiansNeg1,
  -30  as degreesNeg30, radians(30) as radiansNeg30,
  -45  as degreesNeg45, radians(45) as radiansNeg45,
  -60  as degreesNeg60, radians(60) as radiansNeg60,
  -90  as degreesNeg90, radians(90) as radiansNeg90,
  -120 as degreesNeg120, radians(120) as radiansNeg120,
  -135 as degreesNeg135, radians(135) as radiansNeg135,
  -180 as degreesNeg180, radians(180) as radiansNeg180,
  -270 as degreesNeg270, radians(270) as radiansNeg270,
  -360 as degreesNeg360, radians(360) as radiansNeg360
from math_test where id=1


