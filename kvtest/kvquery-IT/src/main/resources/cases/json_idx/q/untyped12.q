select id
from bar b
where b.info.address.state = "OR" and
      b.info.address.city >= 5
