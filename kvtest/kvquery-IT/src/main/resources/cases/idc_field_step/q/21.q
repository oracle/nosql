# Test Description: Name expression returns NULL

select id, u.address.test
from complex u
where id=3