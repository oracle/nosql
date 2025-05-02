#sign test
select
  sign(0) as sign0,
  sign(1) as sign1,
  sign(1.0) as sign10,
  sign(-1) as signneg1,
  sign(-1.0) as signneg10,
  sign(0.000001) as sign000001,
  sign(-0.000001) as signneg000001,
  sign(-100) as signneg100,
  sign(102234) as sign102234
from math_test where id=1

