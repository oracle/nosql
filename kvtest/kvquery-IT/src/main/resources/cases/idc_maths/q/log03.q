#Negative tests for log
select
  trunc(log(24,0),4) as log_24_0,
  trunc(log(64,1),4) as log_64_1,
  trunc(log(0,0),4) as log_0_0,
  trunc(log(0,1),4) as log_0_1,
  trunc(log(0,-1),4) as log_0_neg1,
  trunc(log(-4,-2),4) as log_neg4_neg2,
  trunc(log(-100,10),4) as log_neg100_10,
  trunc(log(4,-2),4) as log_4_neg2
from functional_test where id =1
