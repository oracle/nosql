# Test Description:: Low and high expressions are missing.

select id, c.address.phones[:]
from complex c
order by id