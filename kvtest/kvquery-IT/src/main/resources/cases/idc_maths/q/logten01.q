#test for logten
select
  trunc(log10(0),7) as logten0,
  trunc(log10(1),7) as logten1,
  trunc(log10(2),7) as logten2,
  trunc(log10(5),7) as logten5,
  trunc(log10(10),7) as logten10,
  trunc(log10(100),7) as logten100,
  trunc(log10(10000),7) as logten10000,
  trunc(log10(1000000000000),7) as logten1000000000000,
  trunc(log10(1/10.0),7) as logten01,
  trunc(log10(1/100.0),7) as logten001,
  trunc(log10(1/1000.0),7) as logten0001,
  trunc(log10(0.00001),7) as logten00001,
  trunc(log10(123.456),7) as logten1230456,
  trunc(log10(-1),7) as logtenneg1,
  trunc(log10(-0.5),7) as logtenneg05,
  trunc(log10(-1000),7) as logtenneg1000,
  trunc(log10(-0.1),7) as logtenneg01,
  trunc(log10(power(10,1000000)),7) as logtenpow
from functional_test where id=1

