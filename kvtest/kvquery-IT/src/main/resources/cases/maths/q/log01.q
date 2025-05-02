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
  
  trunc(log(null,10),4) as log_null_10,
  trunc(log(10, null),4) as log_10_null,
  trunc(log(null,null),4) as log_null_null

from math_test where id=1
  
  
  
