# Test Description: Test for external variable of type LONG.


declare $longmin long;
select income < $longmin
from Users
order by id