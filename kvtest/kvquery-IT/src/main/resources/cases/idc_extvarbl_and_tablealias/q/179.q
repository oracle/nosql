# Test Description: Test for external variable of type DOUBLE.


declare $doublemax double;
select * from Users where income < $doublemax 
order by id