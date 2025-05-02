#acos test with a cos function
select acos(cos(0)) = 0,
  trunc(acos(cos(pi()/6)),4) = trunc(pi()/6,4),
  trunc(acos(cos(pi()/4)),7) = trunc(pi()/4,7),
  trunc(acos(cos(pi()/3)),7) = trunc(pi()/3,7),
  trunc(acos(cos(pi()/2)),7) = trunc(pi()/2,7),
  trunc(acos(cos(pi())),7) = trunc(pi(),7) from functional_test where id=1
