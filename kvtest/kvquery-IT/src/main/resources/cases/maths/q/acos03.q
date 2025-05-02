
select acos(cos(0)) = 0,
  trunc(acos(cos(pi()/6)),4) = trunc(pi()/6,4),
  acos(cos(pi()/4)) = pi()/4,
  acos(cos(pi()/3)) = pi()/3,
  acos(cos(pi()/2)) = pi()/2,
  acos(cos(pi())) = pi() from math_test where id=1

