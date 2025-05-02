# Test Description: Test for external variable of type DOUBLE.


declare $doublemin double;
select id, id < $doublemin
from Users
order by id