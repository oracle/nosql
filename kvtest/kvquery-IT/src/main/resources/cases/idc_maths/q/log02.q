#log tests
select
  trunc(log(1000,10),4) as log_1000_10,
  trunc(log(64,2),4) as log_64_2,
  trunc(log(27,3),4) as log_27_3,
  trunc(log(power(10,100),10),4) as log_10E100_10,

  trunc(log(1,10),4) as log_1_10,
  trunc(log(1,2),4) as log_1_2,
  trunc(log(1,20),4) as log_1_20,
  trunc(log(1,16),4) as log_1_16,


  trunc(log(2,1),4) as log_1_1,
  trunc(log(10,1),4) as log_10_1,
  trunc(log(20,1),4) as log_20_1,

  trunc(log(-100,10),4) as log_neg1_10,
  trunc(log(-1024,2),4) as log_neg1024_2,
  trunc(log(100,0),4) as log_100_0,

  trunc(log(2.5,2),4) as log_2o5_2,
  trunc(log(0.5,0.75),4) as log_o5_o75,
  trunc(log(e(),pi()),4) as log_e_pi,


  trunc(log(100,-10),4) as log_100_neg10,
  trunc(log(256,-2),4) as log_256_neg2,

  trunc(log(0,2),4) as log_0_2,
  trunc(log(0.000000000001,10),4) as log_1Eneg10_2,
  trunc(log(9223372036854775807,2),4) as log_long_max_2,
  trunc(log(2,9223372036854775807),4) as log_long_2_max,
  trunc(log(9223372036854775807,9223372036854775807),4) as log_long_max_max,
  trunc(log(4.7976931348623157e+308,2),4) as log_inf_2,
  trunc(log(4.7976931348623157e+308,4.7976931348623157e+30),4) as log_inf_inf,
  trunc(log(2,4.7976931348623157e+30),4) as log_2_inf
from functional_test where id=1