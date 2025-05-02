#random known values test for ln
select
  trunc(ln(0),7) as ln0,
  trunc(ln(1),7) as ln1,
  trunc(ln(2),7) as ln2,
  trunc(ln(exp(5)),7) as lnexp5,
  trunc(ln(10),7) as ln10,
  trunc(ln(100),7) as ln100,
  trunc(ln(10000),7) as ln10000,
  trunc(ln(exp(1000000000000)),7) as lnexp1000000000000,
  trunc(ln(1/10.0),7) as ln01,
  trunc(ln(1/100.0),7) as ln001,
  trunc(ln(1/1000.0),7) as ln0001,
  trunc(ln(exp(0.00001)),7) as lnexp0o00001,
  trunc(ln(123.456),7) as ln123o456,
  trunc(ln(-1),7) as lnneg1,
  trunc(ln(-0.5),7) as lnneg05,
  trunc(ln(-1000),7) as lnneg1000,
  trunc(ln(-0.1),7) as lnneg01,
  trunc(ln(power(10,1000000)),7) as lnpow
from functional_test where id=1

