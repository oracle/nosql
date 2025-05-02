#exp tests for common numbers
select 
  exp(0) as exp0,
  trunc(exp(1),5) as exp1,
  trunc(exp(2),5) as exp2,
  trunc(exp(5),5) as exp5,
  trunc(exp(10),5) as exp10,
  trunc(exp(100),5) as exp100,
  trunc(exp(1000),5) as exp1000,
  trunc(exp(-1),5) as expneg1,
  trunc(exp(-2),5) as expneg2,
  trunc(exp(-1000),5) as expneg1000,
  trunc(exp(pi()),5) as exppi,
  trunc(exp(0.001),5) as exp001,
  trunc(exp(0.2),5) as exp02,
  trunc(exp(-0.5),5) as expneg05,
  trunc(exp(0.0125),5) as exp00125
from math_test where id=1
  
