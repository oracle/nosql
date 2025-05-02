# Test Description: Test for external variable of type Float.


declare $floatmin float;
select * from Users where id > $floatmin
order by id