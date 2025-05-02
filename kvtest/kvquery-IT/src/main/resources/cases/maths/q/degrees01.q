#radians to degrees of standard angles
select
  degrees(0) as deg0,
  degrees(pi()/180) as deg1,
  degrees(10*pi()/180) as deg10,
  degrees(pi()/6) as deg30,
  degrees(pi()/4) as deg45,
  degrees(pi()/3) as deg60,
  degrees(pi()/2) as deg90,
  degrees(2*pi()/3) as deg120,
  degrees(3*pi()/4) as deg135,
  degrees(pi()) as deg180,
  degrees(3*pi()/2) as deg270,
  degrees(2*pi()) as deg360,
  degrees(-0.0) as degNeg0,
  degrees(-pi()/6) as degNeg30,
  degrees(-pi()/4) as degNeg45,
  degrees(-pi()/3) as degNeg60,
  degrees(-pi()/2) as degNeg90,
  degrees(-2*pi()/3) as degNeg120,
  degrees(-3*pi()/4) as degNeg135,
  degrees(-pi()) as degNeg180,
  degrees(-3*pi()/2) as degNeg270,
  degrees(-2*pi()) as degNeg360
from math_test where id=1


