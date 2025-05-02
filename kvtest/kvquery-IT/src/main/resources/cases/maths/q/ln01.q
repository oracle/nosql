#test for ln 
select
  ln(0) as ln0,
  ln(1) as ln1,
  ln(2) as ln2,
  ln(5) as ln5,
  ln(10) as ln10,
  ln(100) as ln100,
  ln(10000) as ln10000,
  ln(exp(100)) as lne100,
  ln(exp(10)) as lne10,
  ln(0.5) as ln05,
  ln(0.25) as ln025,
  ln(0.125) as ln0125,
  ln(123.456) as ln1230456,
  ln(100000000000) as ln100000000000,
  ln(-1) as lnneg1,
  ln(-0.5) as lnneg05,
  ln(-1000) as lnneg1000
from math_test where id=1
  
