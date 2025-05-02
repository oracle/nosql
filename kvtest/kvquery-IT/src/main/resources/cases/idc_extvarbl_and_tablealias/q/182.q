# Test Description: Test for external variable of type LONG.


declare $longmax long;
select * from Users where id < $longmax
order by id