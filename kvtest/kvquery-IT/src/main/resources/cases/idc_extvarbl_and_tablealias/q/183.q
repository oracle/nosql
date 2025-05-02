# Test Description: Test for external variable of type INTEGER.


declare $intmax integer;
select * from Users where income < $intmax
order by id