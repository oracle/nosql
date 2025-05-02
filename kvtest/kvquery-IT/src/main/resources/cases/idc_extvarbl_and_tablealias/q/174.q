# Test Description: Test for external variable of type NUMBER.


declare $number number;
select * from Users where income < $number 
order by id