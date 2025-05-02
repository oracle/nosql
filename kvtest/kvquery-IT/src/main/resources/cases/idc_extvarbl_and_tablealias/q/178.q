# Test Description: Test for external variable of type Float.


declare $floatmax float;
select * from Users where id < $floatmax
order by id