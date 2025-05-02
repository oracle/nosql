select id, income
from Users2 u
where u.address.state = "CA" and
      u.address.city >= "S" and
      10 < income and income < 20
