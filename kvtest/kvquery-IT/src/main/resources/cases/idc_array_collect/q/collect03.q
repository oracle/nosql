#For each country find all the users and their states with income > 100000
select u.country, array_collect(
  {
    "id" : u.id,
    "state" : case when EXISTS u.address.state then u.address.state else 'N/A' end
  }
) as HNI
from users u where u.income>100000 group by u.country

