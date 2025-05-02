select id
from bar b
where b.info.address.state = "OR" and
      2.9 <= b.info.address.city and b.info.address.city < 5.5
