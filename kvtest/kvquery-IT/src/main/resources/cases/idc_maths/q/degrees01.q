#radians to degrees of standard angles
select
  trunc(degrees(0),7) as deg0,
  trunc(degrees(pi()/180),7) as deg1,
  trunc(degrees(10*pi()/180),7) as deg10,
  trunc(degrees(pi()/6),7) as deg30,
  trunc(degrees(pi()/4),7) as deg45,
  trunc(degrees(pi()/3),7) as deg60,
  trunc(degrees(pi()/2),7) as deg90,
  trunc(degrees(2*pi()/3),7) as deg120,
  trunc(degrees(3*pi()/4),7) as deg135,
  trunc(degrees(pi()),7) as deg180,
  trunc(degrees(3*pi()/2),7) as deg270,
  trunc(degrees(2*pi()),7) as deg360,
  trunc(degrees(-0.0),7) as degNeg0,
  trunc(degrees(-pi()/6),7) as degNeg30,
  trunc(degrees(-pi()/4),7) as degNeg45,
  trunc(degrees(-pi()/3),7) as degNeg60,
  trunc(degrees(-pi()/2),7) as degNeg90,
  trunc(degrees(-2*pi()/3),7) as degNeg120,
  trunc(degrees(-3*pi()/4),7) as degNeg135,
  trunc(degrees(-pi()),7) as degNeg180,
  trunc(degrees(-3*pi()/2),7) as degNeg270,
  trunc(degrees(-2*pi()),7) as degNeg360
from functional_test where id=1


