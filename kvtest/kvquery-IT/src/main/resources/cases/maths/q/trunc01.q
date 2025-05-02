#tests for trunc
select
  #trunc of positive numbers without 2nd arg
  trunc(3.14149) as trunc_3o14149,
  trunc(5.678) as trunc_5o678,
  trunc(1.2345) as trunc_1o2345,
  trunc(9.90) as trunc_9o90,
  trunc(1.5) as trunc_1o5,
  trunc(2.5) as trunc_2o5,
  trunc(9.5) as trunc_9o5,
  trunc(0.5) as trunc_o5,
  trunc(123456789.987654321) as trunc_123456789o987654321,
  
  #trunc of negative numbers without 2nd arg
  trunc(-3.14149) as trunc_Neg3o14149,
  trunc(-5.678) as trunc_Neg5o678,
  trunc(-1.2345) as trunc_Neg1o2345,
  trunc(-9.90) as trunc_Neg9o90,
  trunc(-1.5) as trunc_Neg1o5,
  trunc(-2.5) as trunc_Neg2o5,
  trunc(-9.5) as trunc_Neg9o5,
  trunc(-0.5) as trunc_Nego5,
  trunc(-123456789.987654321) as trunc_Neg123456789o987654321,

  #trunc(+,+)
  trunc(3.15259,3) as trunc_3o15259_3,
  trunc(5.678,2) as trunc_5o678_2,
  trunc(1.2345,1) as trunc_1o2345_1,
  trunc(15.193,1) as trunc_15o193_1,
  trunc(9.90, 2) as trunc__9o90_2,
  trunc(1.5,3) as trunc_1o5_3,
  trunc(2.5, 0) as trunc_2o5_0,
  trunc(9.5,1) as trunc_9o5_1,
  trunc(0.5,1) as trunc_o5_1,
  trunc(123456789.987654321,4) as trunc_123456789o987654321_4,
  trunc(95.654535334545,2) as trunc_95o654535334545_2,
  trunc(123.456,10) as trunc_123o456_10,
  trunc(19.25,1) as trunc_19o25_1,
  trunc(28.73,1) as trunc_28o73_1,
  trunc(34.32,1) as trunc_34o32_1,
  trunc(45.39,1) as trunc_45o39_1,
  trunc(45.39, 0.512) as trunc_45o39_o512,
  
  #trunc(-, +)
  trunc(-3.15259,3) as trunc_Neg3o15259_3,
  trunc(-5.678,2) as trunc_Neg5o678_2,
  trunc(-1.2345,1) as trunc_Neg1o2345_1,
  trunc(-15.193,1) as trunc_Neg15o193_1,
  trunc(-9.90, 2) as trunc_Neg9o90_2,
  trunc(-1.5,3) as trunc_Neg1o5_3,
  trunc(-2.5, 0) as trunc_Neg2o5_0,
  trunc(-9.5,1) as trunc_Neg9o5_1,
  trunc(-0.5,1) as trunc_Nego5_1,
  trunc(-123456789.987654321,4) as trunc_Neg123456789o987654321_4,
  trunc(-95.654535334545,2) as trunc_Neg95o654535334545_2,
  trunc(-123.456,10) as trunc_Neg123o456_10,
  trunc(-19.25,1) as trunc_Neg19o25_1,
  trunc(-28.73,1) as trunc_Neg28o73_1,
  trunc(-34.32,1) as trunc_Neg34o32_1,
  trunc(-45.39,1) as trunc_Neg45o39_1,

  #trunc(+, -)
  trunc(3.15259,-3) as trunc_3o14159_Neg3,
  trunc(5.678,-2) as trunc_5o768_Neg2,
  trunc(1.2345,-1) as trunc_1o2345_Neg1,
  trunc(15.193,-1) as trunc_5o768_Neg1,
  trunc(9.90, -2) as trunc__9o90_Neg2,
  trunc(1.5,-3) as trunc_1o5_Neg3,
  trunc(2.5, -0) as trunc_2o5_Neg0,
  trunc(9.5,-1) as trunc_9o5_Neg1,
  trunc(0.5,-1) as trunc_o5_Neg1,
  trunc(123456789.987654321,-4) as trunc_123456789o987654321_Neg4,
  trunc(95.654535334545,-2) as trunc_95o654535334545_Neg2,
  trunc(123.456,-10) as trunc_123o456_Neg10,
  trunc(19.25,-1) as trunc_19o25_Neg1,
  trunc(28.73,-1) as trunc_28o73_Neg1,
  trunc(34.32,-1) as trunc_34o32_Neg1,
  trunc(45.39,-1) as trunc_45o39_Neg1,
  
  #trunc(-, -)
  trunc(-3.15259,-3) as trunc_Neg3o14159_Neg2,
  trunc(-5.678,-2) as trunc_Neg5o768_Neg2,
  trunc(-1.2345,-1) as trunc_Neg1o2345_Neg1,
  trunc(-15.193,-1) as trunc_Neg5o768_Neg1,
  trunc(-9.90, -2) as trunc_Neg9o90_Neg2,
  trunc(-1.5,-3) as trunc_Neg1o5_Neg3,
  trunc(-2.5, -0) as trunc_Neg2o5_Neg0,
  trunc(-9.5,-1) as trunc_Neg9o5_Neg1,
  trunc(-0.5,-1) as trunc_Nego5_Neg1,
  trunc(-123456789.987654321,-4) as trunc_Neg123456789o987654321_Neg4,
  trunc(-95.654535334545,-2) as trunc_Neg95o654535334545_Neg2,
  trunc(-123.456,-10) as trunc_Neg123o456_Neg10,
  
  #trunc of NULL
  trunc(null) as trunc_null,
  trunc(null,null) as trunc_null_null,
  trunc(15.192,null) as trunc_15o192_null,
  trunc(null, 2) as trunc_null_2,

  #trunc of NaN, infinity
  trunc(123.456, acos(10)) as trunc_123o456_NaN,
  trunc(acos(10)) as trunc_NaN,
  trunc(cot(0)) as trunc_Infinity,
  trunc(123.456, cot(0)) as trunc_123o456_Infinity,
  trunc(ln(0)) as trunc_NegInfinity,
  trunc(123.456, ln(0)) as trunc_123o456_NegInfinity,

  #trunc when abs(d) > 30
  trunc(123.4567890, 30) as trunc_123o4567890_30,
  trunc(123.4567890, 10000) =  trunc(123.4567890,30) as test_trunc_123o4567890_10000,
  trunc(123.4567890, -30) as trunc_123o4567890_Neg30,
  trunc(123.4567890, -10000.25) = trunc(123.4567890, -30) as test_trunc_123o4567890_Neg10000o25,

  #trunc when d is not int
  trunc(123.4567890, 1.5) = trunc(123.4567890, 1) as test_trunc_123o4567890_1o5,
  trunc(123.4567890, e()) = trunc(123.4567890,2) as test_trunc_123o4567890_E,
  trunc(123.4567890, -pi()) = trunc(123.4567890, -3) as test_trunc_123o4567890_NegPI

from math_test where id=1
  
