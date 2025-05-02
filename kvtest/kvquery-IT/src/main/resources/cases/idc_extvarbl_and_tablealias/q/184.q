# Test Description: Test for external variable of type INTEGER.


declare $intmin integer;
select id, income < $intmin 
from Users
order by id