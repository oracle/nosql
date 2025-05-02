#cos function for standard radians
select
  trunc(cos(0),7) as cos0,
  trunc(cos(pi()/6),7) as cos30,
  trunc(cos(pi()/4),7) as cos45,
  trunc(cos(pi()/3),7) as cos60,
  trunc(cos(pi()/2),7) as cos90,
  trunc(cos(pi()),7) as cos180,
  trunc(cos(3*pi()/2),7) as cos270,
  trunc(cos(2*pi()),7) as cos360,
  trunc(cos(-0.0),7) as cos0neg,
  trunc(cos(-pi()/6),7) as cos30neg,
  trunc(cos(-pi()/4),7) as cos45neg,
  trunc(cos(-pi()/3),7) as cos60neg,
  trunc(cos(-pi()/2),7) as cos90neg,
  trunc(cos(-pi()),7) as cos180neg,
  trunc(cos(-3*pi()/2),7) as cos270neg,
  trunc(cos(-2*pi()),7) as cos360neg
from functional_test where id=1

