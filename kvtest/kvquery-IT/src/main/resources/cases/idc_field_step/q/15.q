# Test Description: Context item is an atomic string in a record.

select id, u.address."city".foo
from complex u
order by id