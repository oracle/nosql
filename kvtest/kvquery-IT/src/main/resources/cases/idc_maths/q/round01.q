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
  round(9223372036854775807, 5) as round_longmax_5,
  round(3.7976931348623157e+308, 5) as round_inf_5,
  round(1.7976931348623157e-308,5) as round_doublemin_5,


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
  round(-9223372036854775808, 5) as round_longmin_5,
  round(-5.7976931348623157e+308, 5) as round_neginf_5,
  round(-1.7976931348623157e-308,5) as round_negdoublemin_5,

  #round(+, -)
  round(3.15259,-3) as round_3o14159_Neg3,
  round(5.678,-2) as round_5o768_Neg2,
  round(1.2345,-1) as round_1o2345_Neg1,
  round(15.193,-1) as round_15o193_Neg1,
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
  trunc(round(9223372036854775807, -5)/2147483647.0,2) as round_longmax_neg5_divide_int_max,
  round(3.7976931348623157e+308, -5) as round_inf_neg5,
  round(1.7976931348623157e-308,-5) as round_doublemin_neg5,

  #round(-, -)
  round(-3.15259,-3) as round_Neg3o14159_Neg2,
  round(-5.678,-2) as round_Neg5o768_Neg2,
  round(-1.2345,-1) as round_Neg1o2345_Neg1,
  round(-15.193,-1) as round_Neg15o193_Neg1,
  round(-9.90, -2) as round_Neg9o90_Neg2,
  round(-1.5,-3) as round_Neg1o5_Neg3,
  round(-2.5, -0) as round_Neg2o5_Neg0,
  round(-9.5,-1) as round_Neg9o5_Neg1,
  round(-0.5,-1) as round_Nego5_Neg1,
  round(-123456789.987654321,-4) as round_Neg123456789o987654321_Neg4,
  round(-95.654535334545,-2) as round_Neg95o654535334545_Neg2,
  round(-123.456,-10) as round_Neg123o456_Neg10,
  trunc(round(-9223372036854775807, -5)/2147483647.0,2) as round_longmin_neg5_divide_int_max,
  round(-5.7976931348623157e+308, -5) as round_neginf_neg5,
  round(-1.7976931348623157e-308,-5) as round_negdoublemin_neg5,

  #round of NULL
  round(null) as round_null,
  round(null,null) as round_null_null,
  round(15.192,null) as round_15o192_null,
  round(null, 2) as round_null_2,
  
  #round of NaN
  round(acos(5)) as round_acos5,
  round(acos(5),asin(5)) as round_acos5_asin5,
  round(15.192,acos(-5)) as round_15o192_acosneg5,
  round(asin(-5), 2) as round_asinneg5_2,
  
  #round for abs(d)>30
  round(123456789.987654321,10000) = round(123456789.987654321,30) as round_123456789o987654321_10000_30,
  round(123456789.987654321,-10000) = round(123456789.987654321,-30) as round_123456789o987654321_neg10000_30,
  round(-123456789.987654321,-10000) = round(-123456789.987654321,-30) as round_neg123456789o987654321_neg10000_30,
  round(-123456789.987654321,10000) = round(-123456789.987654321,30) as round_neg123456789o987654321_10000_30,
  round(123456789.987654321,10000.99999) = round(123456789.987654321,30) as round_123456789o987654321_10000o99999_30,
  round(123456789.987654321,-10000.99999) = round(123456789.987654321,-30) as round_123456789o987654321_neg10000o99999_30,
  round(-123456789.987654321,-10000.99999) = round(-123456789.987654321,-30) as round_neg123456789o987654321_neg10000o99999_30,
  round(-123456789.987654321,10000.99999) = round(-123456789.987654321,30) as round_neg123456789o987654321_10000o99999_30,
  
  #round for d!=integer
  round(123456789.987654321,pi()) = round(123456789.987654321,3) as round_123456789o987654321_pi,
  round(123456789.987654321,-pi()) = round(123456789.987654321,-3) as round_123456789o987654321_negpi,
  round(123456789.987654321,1.999999) = round(123456789.987654321,1) as round_123456789o987654321_1o99999,
  round(123456789.987654321,-1.999999) = round(123456789.987654321,-1) as round_123456789o987654321_neg1o99999,

  #round where d>>>length(n)
  round(123.456,12) as round_123o456_12,
  round(123.456,-12) as round_123o456_neg12,
  round(-9223372036854775808, -20) as round_longmin_neg20,
  round(9223372036854775807, 20) as round_longmax_20,
  round(-1.7976931348623157e-308,-500) as round_negdoublemin_neg500,
  trunc(round(1.7976931348623157e-308,500),7) as round_doublemin_500,
  round(100,-5.7976931348623157e+308) as round_100_neginf,
  round(100,3.7976931348623157e+308) as round_100_inf,


  #round of double or float d
  round(123.456,1.2) as round_123o456_1o2,
  round(123.456,-1.2) as round_123o456_neg1o2,
  round(123,-1.5) as round_123_neg1o5,
  round(123,pi()) as round_123_pi,
  round(123,-pi()) as round_123_negpi


from functional_test where id=1

