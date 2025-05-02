select count(id) as cnt,
       to_last_day_of_month(t3) as day
from roundtest
group by to_last_day_of_month(t3)
