#sqrt tests
select
  sqrt(0) as sqrt0,
  sqrt(1) as sqrt1,
  sqrt(4) as sqrt4,
  sqrt(9) as sqrt9,
  sqrt(25) as sqrt25,
  sqrt(100) as sqrt100,  
  sqrt(1000000000000.0) as sqrt1000000000000,
  sqrt(0.1) as sqrt01,
  sqrt(0.5) as sqrt05,
  sqrt(0.25) as sqrt025,
  sqrt(0.64) as aqrt064,
  sqrt(123.45) as sqrt123045,
  sqrt(-1) as sqrtneg1,
  sqrt(-0.001) as sqrtneg0001,
  sqrt(-1024.0) as sqrtneg1024
from math_test where id=1
  
