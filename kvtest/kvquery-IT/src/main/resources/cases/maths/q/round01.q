#tests for round
select
  #round of positive numbers without 2nd arg
  round(3.14149) as round_3o14149,
  round(5.678) as round_5o678,
  round(1.2345) as round_1o2345,
  round(9.90) as round_9o90,
  round(1.5) as round_1o5,
  round(2.5) as round_2o5,
  round(9.5) as round_9o5,
  round(0.5) as round_o5,
  round(123456789.987654321) as round_123456789o987654321,
  
  #round of negative numbers without 2nd arg
  round(-3.14149) as round_Neg3o14149,
  round(-5.678) as round_Neg5o678,
  round(-1.2345) as round_Neg1o2345,
  round(-9.90) as round_Neg9o90,
  round(-1.5) as round_Neg1o5,
  round(-2.5) as round_Neg2o5,
  round(-9.5) as round_Neg9o5,
  round(-0.5) as round_Nego5,
  round(-123456789.987654321) as round_Neg123456789o987654321,

  #round(+,+)
  round(3.15259,3) as round_3o15259_3,
  round(5.678,2) as round_5o678_2,
  round(1.2345,1) as round_1o2345_1,
  round(15.193,1) as round_15o193_1,
  round(9.90, 2) as round__9o90_2,
  round(1.5,3) as round_1o5_3,
  round(2.5, 0) as round_2o5_0,
  round(9.5,1) as round_9o5_1,
  round(0.5,1) as round_o5_1,
  round(123456789.987654321,4) as round_123456789o987654321_4,
  round(95.654535334545,2) as round_95o654535334545_2,
  round(123.456,10) as round_123o456_10,
  round(19.25,1) as round_19o25_1,
  round(28.73,1) as round_28o73_1,
  round(34.32,1) as round_34o32_1,
  round(45.39,1) as round_45o39_1,
  round(45.39, 0.512) as round_45o39_o512,
  
  #round(-, +)
  round(-3.15259,3) as round_Neg3o15259_3,
  round(-5.678,2) as round_Neg5o678_2,
  round(-1.2345,1) as round_Neg1o2345_1,
  round(-15.193,1) as round_Neg15o193_1,
  round(-9.90, 2) as round_Neg9o90_2,
  round(-1.5,3) as round_Neg1o5_3,
  round(-2.5, 0) as round_Neg2o5_0,
  round(-9.5,1) as round_Neg9o5_1,
  round(-0.5,1) as round_Nego5_1,
  round(-123456789.987654321,4) as round_Neg123456789o987654321_4,
  round(-95.654535334545,2) as round_Neg95o654535334545_2,
  round(-123.456,10) as round_Neg123o456_10,
  round(-19.25,1) as round_Neg19o25_1,
  round(-28.73,1) as round_Neg28o73_1,
  round(-34.32,1) as round_Neg34o32_1,
  round(-45.39,1) as round_Neg45o39_1,

  #round(+, -)
  round(3.15259,-3) as round_3o14159_Neg3,
  round(5.678,-2) as round_5o768_Neg2,
  round(1.2345,-1) as round_1o2345_Neg1,
  round(15.193,-1) as round_5o768_Neg1,
  round(9.90, -2) as round__9o90_Neg2,
  round(1.5,-3) as round_1o5_Neg3,
  round(2.5, -0) as round_2o5_Neg0,
  round(9.5,-1) as round_9o5_Neg1,
  round(0.5,-1) as round_o5_Neg1,
  round(123456789.987654321,-4) as round_123456789o987654321_Neg4,
  round(95.654535334545,-2) as round_95o654535334545_Neg2,
  round(123.456,-10) as round_123o456_Neg10,
  round(19.25,-1) as round_19o25_Neg1,
  round(28.73,-1) as round_28o73_Neg1,
  round(34.32,-1) as round_34o32_Neg1,
  round(45.39,-1) as round_45o39_Neg1,
  
  #round(-, -)
  round(-3.15259,-3) as round_Neg3o14159_Neg2,
  round(-5.678,-2) as round_Neg5o768_Neg2,
  round(-1.2345,-1) as round_Neg1o2345_Neg1,
  round(-15.193,-1) as round_Neg5o768_Neg1,
  round(-9.90, -2) as round_Neg9o90_Neg2,
  round(-1.5,-3) as round_Neg1o5_Neg3,
  round(-2.5, -0) as round_Neg2o5_Neg0,
  round(-9.5,-1) as round_Neg9o5_Neg1,
  round(-0.5,-1) as round_Nego5_Neg1,
  round(-123456789.987654321,-4) as round_Neg123456789o987654321_Neg4,
  round(-95.654535334545,-2) as round_Neg95o654535334545_Neg2,
  round(-123.456,-10) as round_Neg123o456_Neg10,
  
  #round of NULL
  round(null) as round_null,
  round(null,null) as round_null_null,
  round(15.192,null) as round_15o192_null,
  round(null, 2) as round_null_2,

  #round of NaN, infinity
  round(acos(10)) as round_NaN,
  round(123.456, acos(10)) as round_123o456_NaN,
  round(cot(0)) as round_Infinity,
  round(123.456, cot(0)) as round_123o456_Infinity,
  round(ln(0)) as round_NegInfinity,
  round(123.456, ln(0)) as round_123o456_NegInfinity,

  #round when abs(d) > 30
  round(123.4567890, 30) as round_123o4567890_30,
  round(123.4567890, 10000) =  round(123.4567890,30) as test_round_123o4567890_10000,
  round(123.4567890, -30) as round_123o4567890_Neg30,
  round(123.4567890, -10000.25) = round(123.4567890, -30) as test_round_123o4567890_Neg10000o25,

  #round when d is not int
  round(123.4567890, 1.5) = round(123.4567890, 1) as test_round_123o4567890_1o5,
  round(123.4567890, e()) = round(123.4567890,2) as test_round_123o4567890_E,
  round(123.4567890, -pi()) = round(123.4567890, -3) as test_round_123o4567890_NegPI

from math_test where id=1
  
