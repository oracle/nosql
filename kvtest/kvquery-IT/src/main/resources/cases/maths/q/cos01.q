#cos function for standard radians
select 
  cos(0) as cos0,
  cos(pi()/6) as cos30,
  cos(pi()/4) as cos45,
  cos(pi()/3) as cos60,
  cos(pi()/2) as cos90,
  cos(pi()) as cos180,
  cos(3*pi()/2) as cos270,
  cos(2*pi()) as cos360,
  cos(-0.0) as cos0neg,
  cos(-pi()/6) as cos30neg,
  cos(-pi()/4) as cos45neg,
  cos(-pi()/3) as cos60neg,
  cos(-pi()/2) as cos90neg,
  cos(-pi()) as cos180neg,
  cos(-3*pi()/2) as cos270neg,
  cos(-2*pi()) as cos360neg
from math_test where id=1

