select u.country, array_collect(distinct
  {
    "id" : u.id,
    "state" : case when EXISTS u.address.state then u.address.state else 'N/A' end
  }
) as HNI, count(*) as count
from users u where u.income>100000 group by u.country

