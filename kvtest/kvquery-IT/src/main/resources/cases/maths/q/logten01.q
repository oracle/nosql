#test for logten 
select
  log10(0) as logten0,
  log10(1) as logten1,
  log10(2) as logten2,
  log10(5) as logten5,
  log10(10) as logten10,
  log10(100) as logten100,
  log10(10000) as logten10000,
  log10(1000000000000) as logten1000000000000,
  log10(1/10.0) as logten01,
  log10(1/100.0) as logten001,
  log10(1/1000.0) as logten0001,
  log10(0.00001) as logten00001,
  log10(123.456) as logten1230456,
  log10(-1) as logtenneg1,
  log10(-0.5) as logtenneg05,
  log10(-1000) as logtenneg1000,
  log10(-0.1) as logtenneg01,
  log10(power(10,1000000)) as logtenpow
from math_test where id=1
  
